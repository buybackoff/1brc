using System.Diagnostics;
using System.IO.MemoryMappedFiles;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using Microsoft.Win32.SafeHandles;

namespace _1brc
{
    public enum ProcessMode
    {
        Default = 0,
        MmapSingle = 1,
        MmapViewPerChunk = 2,
        MmapViewPerChunkRandom = 3,
    }

    public class App : IDisposable
    {
        private readonly ProcessMode _processMode;
        private readonly FileStream _fileStream;
        private readonly MemoryMappedFile _mmf;
        private readonly MemoryMappedViewAccessor? _va;
        private readonly SafeMemoryMappedViewHandle? _vaHandle;
        private readonly nint _pointer;

        private int _chunkCount;
        private long _leftoverStart;
        private int _leftoverLength;
        private readonly nint _leftoverPtr;

        private int _threadsFinished;
        private readonly SafeFileHandle _fileHandle;
        private readonly List<(long start, int length)> _chunks;

        private const int MAX_CHUNK_SIZE = int.MaxValue - 100_000;

        private const int MAX_VECTOR_SIZE = 32;

        private const int MAX_LINE_SIZE = MAX_VECTOR_SIZE * 4; // 100  ; -  99.9 \n => 107 => 4x Vector size

        private const int LEFTOVER_CHUNK_ALLOC = MAX_LINE_SIZE * 8;

        public string FilePath { get; }

        public unsafe App(string filePath, int? chunkCount = null, ProcessMode processMode = ProcessMode.Default)
        {
            _processMode = processMode;

            _chunkCount = Math.Max(1, chunkCount ?? Environment.ProcessorCount);
#if DEBUG
            _chunkCount = 1;
#endif

            FilePath = filePath;

            _fileStream = new FileStream(FilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite, 1, FileOptions.SequentialScan);
            _fileHandle = _fileStream.SafeFileHandle;
            var fileLength = _fileStream.Length;
            _mmf = MemoryMappedFile.CreateFromFile(FilePath, FileMode.Open);

            _leftoverPtr = Marshal.AllocHGlobal(LEFTOVER_CHUNK_ALLOC);

            if (_processMode == ProcessMode.MmapSingle)
            {
                byte* ptr = (byte*)0;
                _va = _mmf.CreateViewAccessor(0, fileLength, MemoryMappedFileAccess.Read);
                _vaHandle = _va.SafeMemoryMappedViewHandle;
                _vaHandle.AcquirePointer(ref ptr);
                _pointer = (nint)ptr;
            }

            _chunks = SplitIntoMemoryChunks();
        }

        public List<(long start, int length)> SplitIntoMemoryChunks()
        {
            Debug.Assert(_fileStream.Position == 0);

            // We want equal chunks not larger than int.MaxValue
            // We want the number of chunks to be a multiple of CPU count, so multiply by 2
            // Otherwise with CPU_N+1 chunks the last chunk will be processed alone.

            // All chunks must be safe to dereference the largest possible SIMD vector (e.g. 64 bytes even if AVX512 is not used)
            // For that we leave a very  small chunk at the end, copy it content and process it after the main job is done. 

            var fileLength = _fileStream.Length;

            var chunkCount = _chunkCount;
            var chunkSize = fileLength / chunkCount;
            while (chunkSize > MAX_CHUNK_SIZE)
            {
                chunkCount *= 2;
                chunkSize = fileLength / chunkCount;
            }

            List<(long start, int length)> chunks = new();

            long pos = 0;
            _leftoverStart = 0;
            _leftoverLength = 0;

            for (int i = 0; i < chunkCount; i++)
            {
                int c;

                if (pos + chunkSize >= fileLength)
                {
                    _fileStream.Position = pos + chunkSize - MAX_LINE_SIZE * 2;

                    while ((c = _fileStream.ReadByte()) >= 0 && c != '\n')
                    {
                        _leftoverStart = _fileStream.Position + 1;
                        _leftoverLength = (int)(fileLength - _leftoverStart);
                    }

                    chunks.Add((pos, (int)(fileLength - pos - _leftoverLength)));
                    break;
                }

                var newPos = pos + chunkSize;

                _fileStream.Position = newPos;

                while ((c = _fileStream.ReadByte()) >= 0 && c != '\n')
                {
                }

                newPos = _fileStream.Position;
                var len = newPos - pos;
                chunks.Add((pos, (int)(len)));
                pos = newPos;
            }

            var previous = chunks[0];
            for (int i = 1; i < chunks.Count; i++)
            {
                var current = chunks[i];

                if (previous.start + previous.length != current.start)
                    throw new Exception("Bad chunks");

                if (i == chunks.Count - 1 && current.start + current.length != fileLength - _leftoverLength)
                    throw new Exception("Bad last chunks");

                previous = current;
            }

            _fileStream.Position = 0;

            _chunkCount = chunks.Count;

            return chunks;
        }

        /// <summary>
        /// PLINQ Entry point
        /// </summary>
        public unsafe FixedDictionary<Utf8Span, Summary> ThreadProcessChunk(int id, long start, uint length)
        {
            var threadResult = new FixedDictionary<Utf8Span, Summary>();

            switch (_processMode)
            {
                case ProcessMode.MmapSingle:
                    ProcessChunkMmapSingle(threadResult, start, length);
                    break;
                case ProcessMode.MmapViewPerChunk:
                    ProcessChunkMmapViewPerChunk(threadResult, start, length);
                    break;
                case ProcessMode.MmapViewPerChunkRandom:
                    ProcessChunkMmapViewPerChunkRandom(threadResult, start, length, id);
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }

            if (Interlocked.Increment(ref _threadsFinished) == 1) // first thread finished
            {
                RandomAccess.Read(_fileHandle, new Span<byte>((byte*)_leftoverPtr, _leftoverLength), _leftoverStart);
                var leftOverSafe = new Utf8Span((byte*)_leftoverPtr, (uint)_leftoverLength);
                ProcessSpan(threadResult, leftOverSafe);
            }

            return threadResult;
        }

        public unsafe void ProcessChunkMmapSingle(FixedDictionary<Utf8Span, Summary> resultAcc, long start, uint length)
        {
            ProcessSpan(resultAcc, new Utf8Span((byte*)_pointer + start, length));
        }

        public unsafe void ProcessChunkMmapViewPerChunk(FixedDictionary<Utf8Span, Summary> resultAcc, long start, uint length)
        {
            using var accessor = _mmf.CreateViewAccessor(start, length + _leftoverLength, MemoryMappedFileAccess.Read);
            byte* ptr = default;
            accessor.SafeMemoryMappedViewHandle.AcquirePointer(ref ptr);
            ProcessSpan(resultAcc, new Utf8Span(ptr + accessor.PointerOffset, length));
            accessor.SafeMemoryMappedViewHandle.ReleasePointer();
        }

        public unsafe void ProcessChunkMmapViewPerChunkRandom(FixedDictionary<Utf8Span, Summary> resultAcc, long start, uint length, int id)
        {
            var ratio = (double)(id + 1) / _chunks.Count;
            var delta = (long)(0.45 * length * ratio);
            var length0 = length / 2 + delta;
            using (var accessor = _mmf.CreateViewAccessor(start, length0 + 1024, MemoryMappedFileAccess.Read))
            {
                
                byte* ptr = default;
                accessor.SafeMemoryMappedViewHandle.AcquirePointer(ref ptr);
                ptr += accessor.PointerOffset;

                var span = new Span<byte>(ptr, (int)length0 - 1024);
                length0 = span.LastIndexOf((byte)'\n') + 1;
                ProcessSpan(resultAcc, new Utf8Span(ptr, (uint)length0));
                accessor.SafeMemoryMappedViewHandle.ReleasePointer();
            }

            using (var accessor = _mmf.CreateViewAccessor(start + length0, (length - length0) + _leftoverLength, MemoryMappedFileAccess.Read))
            {
                // var spanResult = new FixedDictionary<Utf8Span, Summary>();
                length0 = (length - length0);
                byte* ptr = default;
                accessor.SafeMemoryMappedViewHandle.AcquirePointer(ref ptr);
                ptr += accessor.PointerOffset;

                ProcessSpan(resultAcc, new Utf8Span(ptr, (uint)length0));
                accessor.SafeMemoryMappedViewHandle.ReleasePointer();
            }
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        public static unsafe void ProcessSpan(FixedDictionary<Utf8Span, Summary> result, Utf8Span remaining)
        {
            while (remaining.Length > 0)
            {
                nuint idx = remaining.IndexOfSemicolon();

                result.GetValueRefOrAddDefault(new Utf8Span(remaining.Pointer, idx)).Apply(remaining.ParseInt(idx, out idx));

                remaining = remaining.SliceUnsafe(idx);
            }
        }

        public FixedDictionary<Utf8Span, Summary> Process()
        {
            var result = _chunks
                .AsParallel()
#if DEBUG
                .WithDegreeOfParallelism(1)
#endif
                .Select((tuple, id) => ThreadProcessChunk(id, tuple.start, (uint)tuple.length))
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