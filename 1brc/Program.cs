namespace _1brc;

class Program
{
    static void Main(string[] args)
    {
        using var app = new App("D:/tmp/1brc_1B.txt");
        // Console.WriteLine($"Chunk count: {app.SplitIntoMemoryChunks().Count}");
        app.PrintResult();
    }
}