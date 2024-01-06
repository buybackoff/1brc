using System.Buffers;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Text;

namespace _1brc
{
    public unsafe class App : IDisposable
    {
        private readonly FileStream _fileStream;
        private readonly long _fileLength;
        private readonly int _initialChunkCount;

        private readonly Dictionary<string, Summary> _result = new(10000);

        private const int MaxChunkSize = int.MaxValue - 100_000;

        public string FilePath { get; }

        private const int SEGMENT_SIZE = 2 * 1024 * 1024;

        // Refill the buffer when remaining is below than this
        private const int MIN_REMAINING_SIZE = 1024;

        public App(string filePath, int? chunkCount = null)
        {
            _initialChunkCount =
                Math.Max(1, chunkCount ?? Environment.ProcessorCount); // For Non-HT CPUs this is probably better to be 2x => do stuff when another thread waits for IO
            FilePath = filePath;

            _fileStream = new FileStream(FilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite, 1, FileOptions.RandomAccess | FileOptions.Asynchronous);
            var fileLength = _fileStream.Length;
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

#if DEBUG
            chunks.Add((0, (int)_fileLength));
            return chunks;
#endif

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
            Debug.WriteLine($"CHUNKS: {chunks.Count} chunks in {sw.Elapsed}");
            return chunks;
        }

        public int ProcessChunk(long start, int length)
        {
            var buffer = ArrayPool<byte>.Shared.Rent(SEGMENT_SIZE);
            var segmentResult = new Dictionary<Utf8Span, Summary>(10000);
            var chunkRemaining = length;

            var segmentOffset = 0;

            fixed (byte* segmentPtr = &buffer[0])
            {
                while (chunkRemaining > 0)
                {
                    int bufferLength = Math.Min(SEGMENT_SIZE - segmentOffset, chunkRemaining);
                    var bufferSpan = buffer.AsSpan(segmentOffset, bufferLength);

                    var segmentConsumed = RandomAccess.Read(_fileStream.SafeFileHandle, bufferSpan, start);
                    chunkRemaining -= segmentConsumed;
                    start += segmentConsumed;

                    var remaining = new Utf8Span(segmentPtr, segmentOffset + segmentConsumed); // from 0 + copied remainder + segmentConsumed 
#if DEBUG
                    var str = remaining.ToString();
#endif

                    var loopLimit = chunkRemaining == 0 ? 0 : MIN_REMAINING_SIZE;

                    while (remaining.Length > loopLimit)
                    {
                        var idx = remaining.Span.IndexOf((byte)';');
                        ref var summary = ref CollectionsMarshal.GetValueRefOrAddDefault(segmentResult, new Utf8Span(remaining.Pointer, idx), out var exists);
                        var value = remaining.ConsumeNumberWithNewLine(idx + 1, out idx);
                        summary.Apply(value, exists);
                        remaining = remaining.SliceUnsafe(idx);
                    }

                    DumpSegmentResult(segmentResult);

                    if (chunkRemaining > 0)
                    {
                        remaining.Span.CopyTo(buffer); // Copy to the start of the buffer
                        segmentOffset = remaining.Length;
                    }
                    else
                    {
                        break;
                    }
                }
            }

            ArrayPool<byte>.Shared.Return(buffer);

            return 0;
        }

        private void DumpSegmentResult(Dictionary<Utf8Span, Summary> segmentResult)
        {
            lock (_result)
            {
                foreach (KeyValuePair<Utf8Span, Summary> pair in segmentResult)
                {
                    var keyStr = pair.Key.ToString();
                    ref var summary = ref CollectionsMarshal.GetValueRefOrAddDefault(_result, keyStr, out bool exists);
                    if (exists)
                        summary.Merge(pair.Value);
                    else
                        summary = pair.Value;
                }
            }

            segmentResult.Clear();
        }

        public void Process()
        {
            // var tasks = SplitIntoMemoryChunks()
            //     .Select(tuple => Task.Factory.StartNew(() => ProcessChunk(tuple.start, tuple.length), TaskCreationOptions.None))
            //     .ToList();
            // Task.WhenAll(tasks).Wait();

            _ = SplitIntoMemoryChunks()
                .AsParallel()
#if DEBUG
                .WithDegreeOfParallelism(1)
#endif
                .Select((tuple => ProcessChunk(tuple.start, tuple.length)))
                .AsEnumerable()
                .Sum();
        }

        public void PrintResult()
        {
            Process();

            long count = 0;
            Console.OutputEncoding = Encoding.UTF8;
            Console.Write("{");
            var line = 0;

            // ReSharper disable once InconsistentlySynchronizedField
            Dictionary<string, Summary> result = _result;

            foreach (var pair in result.OrderBy(x => x.Key, StringComparer.InvariantCulture))
            {
                count += pair.Value.Count;
                Console.Write($"{pair.Key} = {pair.Value}");
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
            _fileStream.Dispose();
        }
    }
}