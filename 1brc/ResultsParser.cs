using System.Runtime.Serialization;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace _1brc
{
    /// <summary>
    /// Extract results from https://github.com/buybackoff/1brc-bench/ to a csv
    /// </summary>
    public class ResultsParser
    {
        private record struct ResultKey(string Cpu, string Dataset, string User, int Cores, int Threads);

        private readonly Dictionary<ResultKey, List<double>> _results = new();

        public void ExtractToCsv(string dirPath)
        {
            var batchDirs = Directory.EnumerateDirectories(dirPath);
            foreach (string batchDir in batchDirs)
            {
                var dirName = Path.GetFileName(batchDir);
                var cpu = dirName[..dirName.IndexOf('_')];

                var batchDir1B = Path.Combine(batchDir, "1B");

                if (Directory.Exists(batchDir1B))
                    ExtractBatch(cpu, "1B", batchDir1B);

                var batchDir1B10K = Path.Combine(batchDir, "1B_10K");
                if (Directory.Exists(batchDir1B10K))
                    ExtractBatch(cpu, "1B_10K", batchDir1B10K);
            }

            var summary =
                _results
                    .OrderBy(x => x.Key.Cpu)
                    .ThenBy(x => x.Key.Dataset)
                    .ThenBy(x => x.Key.User)
                    .ThenBy(x => x.Key.Cores)
                    .ThenBy(x => x.Key.Threads)
                    .Select(x => (x.Key, Summarize(x.Value)));

            var outputPath = Path.Combine(dirPath, "results.csv");
            File.Delete(outputPath);
            using var writer = File.AppendText(outputPath);
            writer.WriteLine($"{nameof(ResultKey.Cpu)},{nameof(ResultKey.Dataset)},{nameof(ResultKey.User)},{nameof(ResultKey.Cores)},{nameof(ResultKey.Threads)},Min,Avg,StDev");
            foreach ((ResultKey Key, (double Min, double Avg, double StDev)) in summary)
            {
                // https://github.com/buybackoff/1brc/issues/19
                if (Min < 0.05) // NimaAra for 10K before fix
                    continue;
                if (Key.User == "dzaima" && Key.Threads > 8) //  Bad ENV with sudo
                    continue;

                writer.WriteLine($"{Key.Cpu},{Key.Dataset},{Key.User},{Key.Cores},{Key.Threads},{Min:N3},{Avg:N3},{StDev:N3}");
            }
        }

        private (double Min, double Avg, double StDev) Summarize(List<double> times)
        {
            var min = times[0];
            var avg = min;
            var stdDev = 0.0;

            if (times.Count <= 3)
            {
                avg = times.Average();
                stdDev = StdDev(times);
            }
            else if (times.Count < 25)
            {
                times = times.Skip(1).Take(times.Count - 2).ToList();
                avg = times.Average();
                stdDev = StdDev(times);
            }
            else
            {
                times = times.Skip(2).Take(times.Count - 5).ToList();
                avg = times.Average();
                stdDev = StdDev(times);
            }

            return (min, avg, stdDev);
        }

        public static double StdDev(IEnumerable<double> values)
        {
            double mean = 0.0;
            double sum = 0.0;
            double stdDev = 0.0;
            int n = 0;
            foreach (double val in values)
            {
                n++;
                double delta = val - mean;
                mean += delta / n;
                sum += delta * (val - mean);
            }

            if (1 < n)
                stdDev = Math.Sqrt(sum / (n - 1));

            return stdDev;
        }

        private void ExtractBatch(string cpu, string dataset, string batchDir)
        {
            var userFiles = Directory.EnumerateFiles(batchDir);
            foreach (var userFilePath in userFiles)
            {
                var fileName = Path.GetFileName(userFilePath);
                var userName = fileName[..fileName.IndexOf('_')];
                var coresStr = fileName.Substring(userName.Length + 1, fileName.IndexOf('_', userName.Length + 1) - (userName.Length + 1));
                var threadsStr = fileName.Substring(userName.Length + 1 + coresStr.Length + 1,
                    fileName.IndexOf('_', userName.Length + 1 + coresStr.Length + 1) - (userName.Length + 1 + coresStr.Length + 1));
                var cores = int.Parse(coresStr);
                var threads = int.Parse(threadsStr);

                List<Result>? results = null;

                try
                {
#pragma warning disable IL2026
#pragma warning disable IL3050
                    string json = File.ReadAllText(userFilePath);
                    var jsonWithResults = JsonSerializer.Deserialize<JsonWithResults>(json);
                    results = jsonWithResults?.Results;
#pragma warning restore IL3050
#pragma warning restore IL2026

                }
                catch
                {
                    ;
                }

                if (results != null && results.Count > 0 && results[0].Times.Count > 0)
                {
                    var times = results[0].Times;
                    var key = new ResultKey(cpu, dataset, userName, cores, threads);
                    if (!_results.TryGetValue(key, out var resultTimes))
                    {
                        resultTimes = new List<double>();
                        _results[key] = resultTimes;
                    }

                    resultTimes.AddRange(times);
                    resultTimes.Sort();
                }
            }
        }

        public class JsonWithResults
        {
            [JsonPropertyName("results")]
            public List<Result> Results { get; set; }
        }

        public class Result
        {
            [JsonPropertyName("times")]
            public List<double> Times { get; set; }
        }
    }
}