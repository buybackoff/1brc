using System.Diagnostics;
using System.IO.MemoryMappedFiles;
using System.Numerics;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Text;
using Microsoft.Win32.SafeHandles;

namespace _1brc
{
    public enum ProcessMode
    {
        Default,
        MmapSingle,
        MmapSingleSharedPos,
        MmapViewPerChunk,
        MmapViewPerChunkRandom,
        RandomAccess,
        RandomAccessAsync
    }

    public class App : IDisposable
    {
        private readonly ProcessMode _processMode;
        private readonly FileStream _fileStream;
        private readonly MemoryMappedFile _mmf;
        private readonly MemoryMappedViewAccessor? _va;
        private readonly SafeMemoryMappedViewHandle? _vaHandle;
        private readonly nint _pointer;

        private int _threadCount;
        private long _leftoverStart;
        private int _leftoverLength;
        private readonly nint _leftoverPtr;

        private long _consumed;

        private int _threadsFinished;
        private readonly SafeFileHandle _fileHandle;
        private readonly List<(long start, long length)> _chunks;

        private const byte LF = (byte)'\n';

        private const int MAX_VECTOR_SIZE = 32;

        private const int MAX_LINE_SIZE = MAX_VECTOR_SIZE * 4; // 100  ; -  99.9 \n => 107 => 4x Vector size

        private const int LEFTOVER_CHUNK_ALLOC = MAX_LINE_SIZE * 8;

        public string FilePath { get; }

        public unsafe App(string filePath, int? threadCount = null, ProcessMode processMode = ProcessMode.Default)
        {
            if (processMode == default)
                processMode = RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? ProcessMode.RandomAccessAsync : ProcessMode.MmapSingleSharedPos;

            _processMode = processMode;

            _threadCount = Math.Max(1, threadCount ?? Environment.ProcessorCount);
#if DEBUG
            _threadCount = 1;
#endif

            FilePath = filePath;

            _fileStream = new FileStream(FilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite, 1, FileOptions.SequentialScan);
            _fileHandle = _fileStream.SafeFileHandle;
            var fileLength = _fileStream.Length;
            _mmf = MemoryMappedFile.CreateFromFile(FilePath, FileMode.Open);

            _leftoverPtr = Marshal.AllocHGlobal(LEFTOVER_CHUNK_ALLOC);

            if (_processMode == ProcessMode.MmapSingle || _processMode == ProcessMode.MmapSingleSharedPos)
            {
                byte* ptr = (byte*)0;
                _va = _mmf.CreateViewAccessor(0, fileLength, MemoryMappedFileAccess.Read);
                _vaHandle = _va.SafeMemoryMappedViewHandle;
                _vaHandle.AcquirePointer(ref ptr);
                _pointer = (nint)ptr;
            }

            _chunks = SplitIntoMemoryChunks();
        }

        public List<(long start, long length)> SplitIntoMemoryChunks()
        {
            Debug.Assert(_fileStream.Position == 0);

            // We want the number of chunks to be equal to CPU count
            // All chunks must be safe to dereference the largest possible SIMD vector (e.g. 64 bytes even if AVX512 is not used)
            // For that we leave a very  small chunk at the end, copy it content and process it after the main job is done. 

            var fileLength = _fileStream.Length;
            var chunkCount = _threadCount;
            var chunkSize = fileLength / chunkCount;

            List<(long start, long length)> chunks = new();

            long pos = 0;
            _leftoverStart = 0;
            _leftoverLength = 0;

            for (int i = 0; i < chunkCount; i++)
            {
                int c;

                if (pos + chunkSize >= fileLength)
                {
                    _fileStream.Position = fileLength - MAX_LINE_SIZE * 2;

                    while ((c = _fileStream.ReadByte()) >= 0 && c != LF)
                    {
                    }

                    _leftoverStart = _fileStream.Position;
                    _leftoverLength = (int)(fileLength - _leftoverStart);

                    chunks.Add((pos, (fileLength - pos - _leftoverLength)));
                    break;
                }

                var newPos = pos + chunkSize;

                _fileStream.Position = newPos;

                while ((c = _fileStream.ReadByte()) >= 0 && c != LF)
                {
                }

                newPos = _fileStream.Position;
                var len = newPos - pos;
                chunks.Add((pos, (len)));
                pos = newPos;
            }

            var previous = chunks[0];
            for (int i = 1; i < chunks.Count; i++)
            {
                var current = chunks[i];

                if (previous.start + previous.length != current.start)
                    throw new Exception($"Bad chunks: {previous.start} + {previous.length} != {current.start}");

                if (i == chunks.Count - 1 && current.start + current.length != fileLength - _leftoverLength)
                    throw new Exception("Bad last chunks");

                previous = current;
            }

            _fileStream.Position = 0;

            _threadCount = chunks.Count;

            return chunks;
        }

        /// <summary>
        /// PLINQ Entry point
        /// </summary>
        public unsafe FixedDictionary<Utf8Span, Summary> ThreadProcessChunk(int id, long start, long length)
        {
            var threadResult = new FixedDictionary<Utf8Span, Summary>();

            switch (_processMode)
            {
                case ProcessMode.MmapSingle:
                    ProcessChunkMmapSingle(threadResult, start, length);
                    break;
                case ProcessMode.MmapSingleSharedPos:
                    ProcessChunkMmapSingleSharedPos(threadResult, start, length);
                    break;
                case ProcessMode.MmapViewPerChunk:
                    ProcessChunkMmapViewPerChunk(threadResult, start, length);
                    break;
                case ProcessMode.MmapViewPerChunkRandom:
                    ProcessChunkMmapViewPerChunkRandom(threadResult, start, length, id);
                    break;
                case ProcessMode.RandomAccess:
                    ProcessChunkRandomAccess(threadResult, start, length);
                    break;
                case ProcessMode.RandomAccessAsync:
                    ProcessChunkRandomAccessAsync(threadResult, start, length);
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }

#if DEBUG
            Console.WriteLine($"Thread {id} finished at: {DateTime.UtcNow:hh:mm:ss.ffffff}");
#endif

            if (Interlocked.Increment(ref _threadsFinished) == 1) // first thread finished
            {
#if DEBUG
                Debug.Assert(_leftoverLength == _fileStream.Length - _leftoverStart, "_leftoverLength == _fileStream.Length - _leftoverStart");
                Console.WriteLine($"LEFTOVER: {_leftoverStart:N0} - {_fileStream.Length:N0} - {_leftoverLength:N0}");
#endif
                RandomAccess.Read(_fileHandle, new Span<byte>((byte*)_leftoverPtr, _leftoverLength), _leftoverStart);
                var leftOverSafe = new Utf8Span((byte*)_leftoverPtr, (uint)_leftoverLength);
                ProcessSpan(threadResult, leftOverSafe);
            }

            return threadResult;
        }

        public unsafe void ProcessChunkMmapSingle(FixedDictionary<Utf8Span, Summary> resultAcc, long start, long length)
        {
            ProcessSpan(resultAcc, new Utf8Span((byte*)_pointer + start, (nuint)length));
        }

        public unsafe void ProcessChunkMmapSingleSharedPos(FixedDictionary<Utf8Span, Summary> resultAcc, long _, long __)
        {
            const int SEGMENT_SIZE = 4 * 1024 * 1024;

            var ptr0 = (byte*)_pointer;
            while (true)
            {
                var end = Interlocked.Add(ref _consumed, SEGMENT_SIZE);
                var start = end - SEGMENT_SIZE;

                if (start >= _leftoverStart)
                    break;

                // Skip partial line, another segment will eat it.
                if (start > 0 && *(ptr0 + start - 1) != LF)
                    start += new ReadOnlySpan<byte>(ptr0 + start, MAX_LINE_SIZE).IndexOf(LF) + 1;

                if (end > _leftoverStart)
                    end = _leftoverStart;

                if (*(ptr0 + end - 1) != LF)
                    end += new ReadOnlySpan<byte>(ptr0 + end, MAX_LINE_SIZE).IndexOf(LF) + 1;

#if DEBUG
                Console.WriteLine($"SEGMENT: {start:N0} - {end:N0} -> {(end - start):N0}");
                if (start > 0)
                    Debug.Assert(ptr0[start - 1] == LF, "ptr0[start - 1] == LF");
                Debug.Assert(ptr0[end - 1] == LF, "ptr0[end - 1] == LF");
#endif

                if (Vector256.IsHardwareAccelerated)
                    ProcessSpan2(resultAcc, new Utf8Span(ptr0 + start, (uint)(end - start)));
                else
                    ProcessSpan(resultAcc, new Utf8Span(ptr0 + start, (uint)(end - start)));

                if (end == _leftoverStart)
                    break;
            }
        }

        public unsafe void ProcessChunkMmapViewPerChunk(FixedDictionary<Utf8Span, Summary> resultAcc, long start, long length)
        {
            using var accessor = _mmf.CreateViewAccessor(start, length + _leftoverLength, MemoryMappedFileAccess.Read);
            byte* ptr = default;
            accessor.SafeMemoryMappedViewHandle.AcquirePointer(ref ptr);
            ProcessSpan(resultAcc, new Utf8Span(ptr + accessor.PointerOffset, (nuint)length));
            accessor.SafeMemoryMappedViewHandle.ReleasePointer();
        }

        public unsafe void ProcessChunkMmapViewPerChunkRandom(FixedDictionary<Utf8Span, Summary> resultAcc, long start, long length, int id)
        {
            var ratio = (double)(id + 1) / _chunks.Count;
            var delta = (long)(0.49 * length * ratio);
            var length0 = length / 2 + delta;
            using (var accessor = _mmf.CreateViewAccessor(start, length0 + 1024, MemoryMappedFileAccess.Read))
            {

                byte* ptr = default;
                accessor.SafeMemoryMappedViewHandle.AcquirePointer(ref ptr);
                ptr += accessor.PointerOffset;

                var span = new Span<byte>(ptr + length0 - 1024, 1024);
                length0 = length0 - 1024 + span.LastIndexOf(LF) + 1;

                ProcessSpan(resultAcc, new Utf8Span(ptr, (nuint)length0));
                accessor.SafeMemoryMappedViewHandle.ReleasePointer();
            }

            using (var accessor = _mmf.CreateViewAccessor(start + length0, (length - length0) + _leftoverLength, MemoryMappedFileAccess.Read))
            {
                length0 = (length - length0);
                byte* ptr = default;
                accessor.SafeMemoryMappedViewHandle.AcquirePointer(ref ptr);
                ptr += accessor.PointerOffset;

                ProcessSpan(resultAcc, new Utf8Span(ptr, (nuint)length0));
                accessor.SafeMemoryMappedViewHandle.ReleasePointer();
            }
        }

        public unsafe void ProcessChunkRandomAccess(FixedDictionary<Utf8Span, Summary> resultAcc, long start, long length)
        {
            using var fileHandle = File.OpenHandle(FilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite, FileOptions.SequentialScan);

            const uint SEGMENT_SIZE = 512 * 1024;

            byte[] buffer = new byte[SEGMENT_SIZE + MAX_LINE_SIZE];

            long chunkRemaining = length;
            fixed (byte* segmentPtr = &buffer[0])
            {
                while (chunkRemaining > 0)
                {
                    var bufferLength = (int)Math.Min(SEGMENT_SIZE, chunkRemaining);
                    Span<byte> bufferSpan = buffer.AsSpan(0, bufferLength);

                    // Just ignore what we've read after \n
                    int segmentRead = RandomAccess.Read(fileHandle, bufferSpan, start);
                    int segmentConsumed = bufferSpan.Slice(0, segmentRead).LastIndexOf(LF) + 1;

                    chunkRemaining -= segmentConsumed;
                    start += segmentConsumed;

                    var remaining = new Utf8Span(segmentPtr, (uint)(segmentConsumed));

                    ProcessSpan(resultAcc, remaining);
                }
            }
        }

        public void ProcessChunkRandomAccessAsync(FixedDictionary<Utf8Span, Summary> resultAcc, long start, long length)
        {
            using var fileHandle = File.OpenHandle(FilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite, FileOptions.Asynchronous | FileOptions.SequentialScan);
            const int SEGMENT_SIZE = 512 * 1024;

            byte[] buffer0 = GC.AllocateArray<byte>(SEGMENT_SIZE + MAX_LINE_SIZE, pinned: true);
            byte[] buffer1 = GC.AllocateArray<byte>(SEGMENT_SIZE + MAX_LINE_SIZE, pinned: true);

            Task.Run(async () =>
            {
                var chunkRemaining = length;

                ValueTask<int> segmentReadTask = ValueTask.FromResult(0);

                var awaitedZero = true;

                while (chunkRemaining > 0)
                {
                    var bufferReadLen = await segmentReadTask;

                    Memory<byte> memoryRead = (awaitedZero ? buffer0 : buffer1).AsMemory(0, bufferReadLen);

                    var segmentConsumed = memoryRead.Span.Slice(0, bufferReadLen).LastIndexOf(LF) + 1;
                    chunkRemaining -= segmentConsumed;
                    start += segmentConsumed;

                    int bufferLengthToRead = (int)Math.Min(SEGMENT_SIZE, chunkRemaining);

                    awaitedZero = !awaitedZero;
                    var memoryToRead = (awaitedZero ? buffer0 : buffer1).AsMemory(0, bufferLengthToRead);

                    segmentReadTask = RandomAccess.ReadAsync(fileHandle, memoryToRead, start);

                    if (segmentConsumed > 0)
                    {
                        var remaining = new Utf8Span(ref memoryRead.Span[0], (uint)(segmentConsumed));
                        ProcessSpan(resultAcc, remaining);

                    }
                }
            }).Wait();
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        public static unsafe void ProcessSpan(FixedDictionary<Utf8Span, Summary> result, Utf8Span remaining)
        {
            while (remaining.Length > 0)
            {
                nuint idx = remaining.IndexOfSemicolon();
                nint value = remaining.ParseInt(idx, out var idx1);
                result.GetValueRefOrAddDefault(new Utf8Span(remaining.Pointer, idx)).Apply(value);
                remaining = remaining.SliceUnsafe(idx1);
            }
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        public static unsafe void ProcessSpan2(FixedDictionary<Utf8Span, Summary> result, Utf8Span remaining)
        {
            Debug.Assert(Vector256.IsHardwareAccelerated);

            const nuint vectorSize = 32;
            var sepVec = Vector256.Create((byte)';');
            var ptr = remaining.Pointer;
            var remLen = remaining.Length;

            while (true)
            {
                if (remLen <= 0)
                    break;

                nuint idx;
                nuint idx1;
                nint value;
                var matches = Vector256.Equals(Unsafe.ReadUnaligned<Vector256<byte>>(ptr), sepVec);
                var mask = Vector256.ExtractMostSignificantBits(matches);

                if (mask != 0)
                {
                    idx = (nuint)BitOperations.TrailingZeroCount(mask);
                    value = ParseInt(ptr, idx, out idx1);
                    if (result.TryUpdate(new Utf8Span(ptr, idx), value))
                    {
                        ptr += idx1;
                        remLen -= idx1;
                        continue;
                    }
                }
                else // 32-63
                {
                    matches = Vector256.Equals(Unsafe.ReadUnaligned<Vector256<byte>>(ptr + vectorSize), sepVec);
                    mask = Vector256.ExtractMostSignificantBits(matches);

                    if (mask != 0) // 64-95
                    {
                        idx = vectorSize + (uint)BitOperations.TrailingZeroCount(mask);
                        value = ParseInt(ptr, idx, out idx1);
                    }
                    else
                    {
                        matches = Vector256.Equals(Unsafe.ReadUnaligned<Vector256<byte>>(ptr + 2 * vectorSize), sepVec);
                        mask = Vector256.ExtractMostSignificantBits(matches);

                        if (mask != 0) // 96-127
                        {
                            idx = 2 * vectorSize + (uint)BitOperations.TrailingZeroCount(mask);
                            value = ParseInt(ptr, idx, out idx1);
                        }
                        else
                        {
                            matches = Vector256.Equals(Unsafe.ReadUnaligned<Vector256<byte>>(ptr + 3 * vectorSize), sepVec);
                            mask = Vector256.ExtractMostSignificantBits(matches);
                            idx = 3 * vectorSize + (uint)BitOperations.TrailingZeroCount(mask);
                            value = ParseInt(ptr, idx, out idx1);
                        }
                    }
                }

                result.GetValueRefOrAddDefault(new Utf8Span(ptr, idx)).Apply(value);
                ptr += idx1;
                remLen -= idx1;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            static nint ParseInt(byte* ptr, nuint start, out nuint lfIndex)
            {
                const long DOT_BITS = 0x10101000;
                const long MAGIC_MULTIPLIER = (100 * 0x1000000 + 10 * 0x10000 + 1);

                long word = *(long*)(ptr + start + 1);
                long inverted = ~word;
                int dot = BitOperations.TrailingZeroCount(inverted & DOT_BITS);
                long signed = (inverted << 59) >> 63;
                long mask = ~(signed & 0xFF);
                long digits = ((word & mask) << (28 - dot)) & 0x0F000F0F00L;
                long abs = ((digits * MAGIC_MULTIPLIER) >>> 32) & 0x3FF;
                var value = ((abs ^ signed) - signed);
                lfIndex = start + (uint)(dot >> 3) + 4u;
                return (nint)value;
            }
        }

        public FixedDictionary<Utf8Span, Summary> Process()
        {
#if DEBUG
            Console.WriteLine($"CHUNKS: {_chunks.Count}");
#endif

            var result = _chunks
                .AsParallel()
#if DEBUG
                .WithDegreeOfParallelism(1)
#endif
                .Select((tuple, id) => ThreadProcessChunk(id, tuple.start, tuple.length))
                .Aggregate((result, chunk) =>
                {
                    foreach (KeyValuePair<Utf8Span, Summary> pair in chunk)
                    {
                        result.GetValueRefOrAddDefault(pair.Key).Merge(pair.Value);
                    }

                    chunk.Dispose();
                    return result;
                });

            return result;
        }

        public void PrintResult()
        {
            var result = Process();

            ulong count = 0;
            Console.OutputEncoding = Encoding.UTF8;

            var sb = new StringBuilder();

            sb.Append("{");

            var ordered = result
                    .Select(x => (Name: x.Key.ToString(), x.Value))
                    .OrderBy(x => x.Name, StringComparer.Ordinal)
                ;

            var first = true;
            foreach (var pair in ordered)
            {
                count += pair.Value.Count;

                if (!first) sb.Append(", ");
                first = false;

                sb.Append($"{pair.Name}={pair.Value}");
            }

            sb.Append("}");

            var strResult = sb.ToString();
            // File.WriteAllText($"D:/tmp/results/buybackoff_{DateTime.Now:yy-MM-dd_hhmmss}.txt", strResult);
            Console.WriteLine(strResult);

            result.Dispose();

            if (count != 1_000_000_000)
                Console.WriteLine($"Total row count {count:N0}");
        }

        public void Dispose()
        {
            Marshal.FreeHGlobal(_leftoverPtr);
            _vaHandle?.ReleasePointer();
            _vaHandle?.Dispose();
            _va?.Dispose();
            _mmf.Dispose();
            _fileHandle.Dispose();
            _fileStream.Dispose();
        }
    }
}