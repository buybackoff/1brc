using System.Diagnostics;
using System.Numerics;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;
using System.Text;

namespace _1brc;

internal class Program
{
    private static void Main(string[] args)
    {
        var sw = Stopwatch.StartNew();
        Console.OutputEncoding = Encoding.UTF8;
        var path = args.Length > 0 ? args[0] : "D:/tmp/measurements_1B_10K.txt";
        using (var app = new App(path))
        {
            app.PrintResult();
        }
        sw.Stop();
        Console.WriteLine($"Finished in: {sw.ElapsedMilliseconds:N0} ms");
    }
}