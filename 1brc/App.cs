using System.Diagnostics;
using System.IO.MemoryMappedFiles;
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

        public App(string filePath, int? chunkCount = null)
        {
            _initialChunkCount = Math.Max(1, chunkCount ?? Environment.ProcessorCount);
            FilePath = filePath;

            _fileStream = new FileStream(FilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite, 1, FileOptions.RandomAccess);
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
            var sw = Stopwatch.StartNew();
            Debug.Assert(_fileStream.Position == 0);

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

                _fileStream.Position = newPos;

                int c;
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

                if (i == chunks.Count - 1 && current.start + current.length != _fileLength)
                    throw new Exception("Bad last chunks");

                previous = current;
            }

            _fileStream.Position = 0;

            sw.Stop();
            Debug.WriteLine($"CHUNKS {sw.Elapsed}");
            return chunks;
        }

        public Dictionary<Utf8Span, Summary> ProcessChunk(long start, int length)
        {
            // Console.WriteLine($"{start};{length}");
            // return new Dictionary<Utf8Span, Summary>();
            // using var va = _mmf.CreateViewAccessor(start, length, MemoryMappedFileAccess.Read);
            // using var vaHandle = va.SafeMemoryMappedViewHandle;
            // byte* ptr = default;
            // vaHandle.AcquirePointer(ref ptr);

            // long offset = ptr - _pointer;
            // Debug.Assert(offset == start, $"offset {offset} == start {start}");

            var result = new Dictionary<Utf8Span, Summary>(10000);
            var remaining = new Utf8Span(_pointer + start, length);
            // var str = remaining.ToString();

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
#if DEBUG
                .WithDegreeOfParallelism(1)
#endif
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

        public void Dispose()
        {
            _vaHandle.Dispose();
            _va.Dispose();
            _mmf.Dispose();
            _fileStream.Dispose();
        }
    }
}