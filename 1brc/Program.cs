using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Text;

namespace _1brc;

internal class Program
{
    private static void Main(string[] args)
    {
        var path = args.Length > 0 ? args[0] : "D:/tmp/measurements_1B_10K.txt";

        Console.OutputEncoding = Encoding.UTF8;

        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows) || args.Contains("--worker"))
            DoWork(path);
        else
            StartSubprocess(path);
    }

    private static void DoWork(string path)
    {
        using (var app = new App(path))
        {
            var sw = Stopwatch.StartNew();
            app.PrintResult();
            sw.Stop();
            Console.Out.Close();
        }
    }

    private static void StartSubprocess(string path)
    {
        string parentProcessPath = Process.GetCurrentProcess().MainModule!.FileName;

        Console.WriteLine($"CMD: -c \"{parentProcessPath} {path} --worker & \" ");

        var processStartInfo = new ProcessStartInfo
        {
            FileName = "sh", // parentProcessPath,
            Arguments = $"-c \"{parentProcessPath} {path} --worker & \" ",
            RedirectStandardOutput = true,
            UseShellExecute = false,
            CreateNoWindow = false,
        };

        var process = Process.Start(processStartInfo);
        string? output = process!.StandardOutput.ReadLine();
        Console.Write(output);
    }
}