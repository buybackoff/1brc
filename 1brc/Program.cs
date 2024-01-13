using System.Diagnostics;

namespace _1brc;

internal class Program
{
    private static void Main(string[] args)
    {
        var sw = Stopwatch.StartNew();
        var path = args.Length > 0 ? args[0] : "D:/tmp/1brc_1B.txt";
        using (var app = new App(path))
        {
            app.PrintResult();  
        }
        sw.Stop();
        Console.WriteLine($"Finished in: {sw.Elapsed}");
    }
}