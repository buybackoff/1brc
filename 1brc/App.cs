using System.Diagnostics;
using System.IO.MemoryMappedFiles;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Text;
using Microsoft.Win32.SafeHandles;

[module: SkipLocalsInit]

namespace _1brc
{
    public class App : IDisposable
    {
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
        private readonly bool _useMmap;

        private const byte LF = (byte)'\n';

        private const int MAX_VECTOR_SIZE = 32;

        private const int MAX_LINE_SIZE = MAX_VECTOR_SIZE * 4; // 100  ; -  99.9 \n => 107 => 4x Vector size

        private const int LEFTOVER_CHUNK_ALLOC = MAX_LINE_SIZE * 8;

        public string FilePath { get; }

        public unsafe App(string filePath, int? threadCount = null, bool? useMmap = null)
        {
            _useMmap = useMmap ?? !RuntimeInformation.IsOSPlatform(OSPlatform.Windows);
            
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

            if (_useMmap)
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
        public unsafe FixedDictionary<Utf8Span, Summary> ThreadProcessChunk(long start, long length)
        {
            var threadResult = new FixedDictionary<Utf8Span, Summary>();

            if (_useMmap)
                ProcessChunkMmap(threadResult, start, length);
            else
                ProcessChunkRandomAccess(threadResult, start, length);

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
        
        public unsafe void ProcessChunkMmap(FixedDictionary<Utf8Span, Summary> resultAcc, long _, long __)
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
                    ProcessSpanX2(resultAcc, new Utf8Span(ptr0 + start, (uint)(end - start)));
                else
                    ProcessSpan(resultAcc, new Utf8Span(ptr0 + start, (uint)(end - start)));

                if (end == _leftoverStart)
                    break;
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

        [MethodImpl(MethodImplOptions.NoInlining)]
        public static unsafe void ProcessSpan(FixedDictionary<Utf8Span, Summary> result, Utf8Span remaining)
        {
            while (remaining.Length > 0)
            {
                nuint idx = remaining.IndexOfSemicolon();
                nint value = remaining.ParseInt(idx, out var nextStart);
                result.Update(new Utf8Span(remaining.Pointer, idx), value);
                remaining = remaining.SliceUnsafe(nextStart);
            }
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        public static void ProcessSpanX2(FixedDictionary<Utf8Span, Summary> result, Utf8Span chunk)
        {
            nuint middle = chunk.Length / 2;
            middle += (uint)chunk.SliceUnsafe(middle).Span.IndexOf((byte)'\n') + 1;
            var chunk0 = chunk.SliceUnsafe(0, (uint)middle);
            var chunk1 = chunk.SliceUnsafe((uint)middle);
            ProcessSpan(result, chunk0, chunk1);
        }

        [MethodImpl(MethodImplOptions.AggressiveOptimization)]
        public static unsafe void ProcessSpan(FixedDictionary<Utf8Span, Summary> result, Utf8Span chunk0, Utf8Span chunk1)
        {
            var ptr0 = chunk0.Pointer;
            var ptrEnd0 = ptr0 + chunk0.Length;
            var ptr1 = chunk1.Pointer;
            var ptrEnd1 = ptr1 + chunk1.Length;

            while (true)
            {
                if (ptr0 >= ptrEnd0)
                    break;

                if (ptr1 >= ptrEnd1)
                    break;

                nuint idx0 = Utf8Span.IndexOfSemicolon(ptr0);
                nuint idx1 = Utf8Span.IndexOfSemicolon(ptr1);

                nint value0 = Utf8Span.ParseInt(ptr0, idx0, out var nextStart0);
                nint value1 = Utf8Span.ParseInt(ptr1, idx1, out var nextStart1);

                result.Update(new Utf8Span(ptr0, idx0), value0);
                result.Update(new Utf8Span(ptr1, idx1), value1);

                ptr0 += nextStart0;
                ptr1 += nextStart1;
            }

            ProcessSpan(result, new Utf8Span(ptr0, (nuint)(ptrEnd0 - ptr0)));
            ProcessSpan(result, new Utf8Span(ptr1, (nuint)(ptrEnd1 - ptr1)));
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
                .Select((tuple) => ThreadProcessChunk(tuple.start, tuple.length))
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