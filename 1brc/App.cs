using System.Diagnostics;
using System.IO.MemoryMappedFiles;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
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

            _fileStream = new FileStream(FilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite, 1, FileOptions.None);
            var fileLength = _fileStream.Length;
            _mmf = MemoryMappedFile.CreateFromFile(_fileStream, $@"{Path.GetFileName(FilePath)}", fileLength, MemoryMappedFileAccess.Read, HandleInheritability.None, true);

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
            var result = new Dictionary<Utf8Span, Summary>();
            var ptr = _pointer + start;
            
            var pos = 0;
            
            while (pos < length)
            {
                var sp = new ReadOnlySpan<byte>(ptr + pos, length);
                
                var sepIdx = sp.IndexOf((byte)';');
                
                var nameUtf8Sp = new Utf8Span(ptr + pos, sepIdx);
                
                ref var summary = ref CollectionsMarshal.GetValueRefOrAddDefault(result, nameUtf8Sp, out bool exists);
                
                sp = sp.Slice(sepIdx + 1);

                var nlIdx = IndexOfNewlineChar(sp, out var stride);

                var valueSp = sp.Slice(0, nlIdx);
                var value = double.Parse(valueSp);
                
                summary.Apply(value, !exists);
                
                pos += sepIdx + 1 + nlIdx + stride;
            }

            return result;
        }


        public Dictionary<Utf8Span, Summary> Process()
        {
            // var tasks = SplitIntoMemoryChunks() // .Skip(1).Take(1)
            //     .Select(tuple => Task.Run(() => ProcessChunk(tuple.start, tuple.length)))
            //     .ToList();
            // var chunks = Task.WhenAll(tasks).Result;
            
            var chunks = SplitIntoMemoryChunks()
                .AsParallel()
                // .WithDegreeOfParallelism(_threads)
                .Select((tuple => ProcessChunk(tuple.start, tuple.length)))
                .ToList();

            Dictionary<Utf8Span, Summary>? result = null;

            foreach (Dictionary<Utf8Span,Summary> chunk in chunks)
            {
                if (result == null)
                {
                    result = chunk;
                    continue;
                }

                foreach (KeyValuePair<Utf8Span,Summary> pair in chunk)
                {
                    ref var summary = ref CollectionsMarshal.GetValueRefOrAddDefault(result, pair.Key, out bool exists);
                    if (exists)
                        summary.Apply(pair.Value);
                    else
                        summary = pair.Value;
                }
            }

            return result!;
        }
        
        public void PrintResult()
        {
            var sw = Stopwatch.StartNew();
            var result = Process();
            foreach (KeyValuePair<Utf8Span,Summary> pair in result.OrderBy(x => x.Key.ToString()))
            {
                Console.WriteLine($"{pair.Key} = {pair.Value}");
            }
            sw.Stop();
            Console.WriteLine($"Processed in {sw.Elapsed}");
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
    }
}