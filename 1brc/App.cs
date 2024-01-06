using System.Diagnostics;
using System.Globalization;
using System.IO.MemoryMappedFiles;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using Microsoft.Win32.SafeHandles;

namespace _1brc
{
    public unsafe class App : IDisposable
    {
        private readonly FileStream _fileStream;
        private readonly MemoryMappedFile _mmf;
        private readonly MemoryMappedViewAccessor _va;
        private readonly SafeMemoryMappedViewHandle _vaHandle;
        private readonly byte* _pointer;
        private readonly long _fileLength;

        private readonly int _initialChunkCount;

        private const int MaxChunkSize = int.MaxValue - 100_000;

        public string FilePath { get; }

        private static double[] _powersOf10 = new double[64];
        private static GCHandle _powersHandle;
        private static readonly double* _powersPtr = Init10Powers();

        public App(string filePath, int? chunkCount = null)
        {
            _initialChunkCount = Math.Max(1, chunkCount ?? Environment.ProcessorCount);
            FilePath = filePath;

            _fileStream = new FileStream(FilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite, 1, FileOptions.SequentialScan);
            var fileLength = _fileStream.Length;
            _mmf = MemoryMappedFile.CreateFromFile(FilePath, FileMode.Open);

            byte* ptr = (byte*)0;
            _va = _mmf.CreateViewAccessor(0, fileLength, MemoryMappedFileAccess.Read);
            _vaHandle = _va.SafeMemoryMappedViewHandle;
            _vaHandle.AcquirePointer(ref ptr);

            _pointer = ptr;
            _fileLength = fileLength;
        }

        public List<(long start, int length)> SplitIntoMemoryChunks()
        {
            // We want equal chunks not larger than int.MaxValue
            // We want the number of chunks to be a multiple of CPU count, so multiply by 2
            // Otherwise with CPU_N+1 chunks the last chunk will be processed alone.

            var chunkCount = _initialChunkCount;
            var chunkSize = _fileLength / chunkCount;
            while (chunkSize > MaxChunkSize)
            {
                chunkCount *= 2;
                chunkSize = _fileLength / chunkCount;
            }

            List<(long start, int length)> chunks = new();

            long pos = 0;

            for (int i = 0; i < chunkCount; i++)
            {
                if (pos + chunkSize >= _fileLength)
                {
                    chunks.Add((pos, (int)(_fileLength - pos)));
                    break;
                }

                var newPos = pos + chunkSize;
                var sp = new ReadOnlySpan<byte>(_pointer + newPos, (int)chunkSize);
                var idx = IndexOfNewlineChar(sp, out var stride);
                newPos += idx + stride;
                var len = newPos - pos;
                chunks.Add((pos, (int)(len)));
                pos = newPos;
            }

            return chunks;
        }

        public Dictionary<Utf8Span, Summary> ProcessChunk(long start, int length)
        {
            var result = new Dictionary<Utf8Span, Summary>(10000);
            var remaining = new Utf8Span(_pointer + start, length);

            while (remaining.Length > 0)
            {
                var idx = remaining.Span.IndexOf((byte)';');
                ref var summary = ref CollectionsMarshal.GetValueRefOrAddDefault(result, new Utf8Span(remaining.Pointer, idx), out var exists);
                var value = remaining.ConsumeNumberWithNewLine(idx + 1, out idx);
                summary.Apply(value, exists);
                remaining = remaining.SliceUnsafe(idx);
            }

            return result;
        }

        public Dictionary<Utf8Span, Summary> Process() =>
            SplitIntoMemoryChunks()
                .AsParallel()
                .Select((tuple => ProcessChunk(tuple.start, tuple.length)))
                .ToList()
                .Aggregate((result, chunk) =>
                {
                    foreach (KeyValuePair<Utf8Span, Summary> pair in chunk)
                    {
                        ref var summary = ref CollectionsMarshal.GetValueRefOrAddDefault(result, pair.Key, out bool exists);
                        if (exists)
                            summary.Merge(pair.Value);
                        else
                            summary = pair.Value;
                    }

                    return result;
                });

        public void PrintResult()
        {
            var result = Process();

            long count = 0;
            Console.OutputEncoding = Encoding.UTF8;
            Console.Write("{");
            var line = 0;
            foreach (var pair in result
                         .Select(x => (Name: x.Key.ToString(), x.Value))
                         .OrderBy(x => x.Name, StringComparer.InvariantCulture))
            {
                count += pair.Value.Count;
                Console.Write($"{pair.Name} = {pair.Value}");
                line++;
                if (line < result.Count)
                    Console.Write(", ");
            }

            Console.WriteLine("}");

            if (count != 1_000_000_000)
                Console.WriteLine($"Total row count {count:N0}");
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static int IndexOfNewlineChar(ReadOnlySpan<byte> span, out int stride)
        {
            stride = default;
            int idx = span.IndexOfAny((byte)'\n', (byte)'\r');
            if ((uint)idx < (uint)span.Length)
            {
                stride = 1;
                if (span[idx] == '\r')
                {
                    int nextCharIdx = idx + 1;
                    if ((uint)nextCharIdx < (uint)span.Length && span[nextCharIdx] == '\n')
                    {
                        stride = 2;
                    }
                }
            }

            return idx;
        }

        public void Dispose()
        {
            _vaHandle.Dispose();
            _va.Dispose();
            _mmf.Dispose();
            _fileStream.Dispose();
        }

        private static double* Init10Powers()
        {
            for (int i = 0; i < 64; i++)
            {
                _powersOf10[i] = 1 / Math.Pow(10, i);
            }

            _powersHandle = GCHandle.Alloc(_powersOf10, GCHandleType.Pinned);
            return (double*)_powersHandle.AddrOfPinnedObject();
        }
    }
}