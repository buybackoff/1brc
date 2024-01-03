namespace _1brc;

class Program
{
    static void Main(string[] args)
    {
        var path = args.Length > 0 ? args[0] : "D:/tmp/1brc_1B.txt";
        using var app = new App(path);
        Console.WriteLine($"Chunk count: {app.SplitIntoMemoryChunks().Count}");
        app.PrintResult();
    }
}