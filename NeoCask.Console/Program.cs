using System.Diagnostics.CodeAnalysis;

namespace NeoCask.Console;

public static class Program
{
    public static void Main(string[] args)
    {
        var directoryName = "G:\\Sharp\\tests_projects\\NeoCask\\test";
        NeoCask neoCask = new NeoCask(directoryName);
        int i = 0;
        while (i < 50)
        {
            neoCask.Put(i.ToString(), new Random().Next().ToString());
            i++;
        }
        
        neoCask.Merge();
        
        int j = 50;
        while (j < 100)
        {
            neoCask.Put(j.ToString(), new Random().Next().ToString());
            j++;
        }
        //
        // var s = neoCask.Get("74");
        // System.Console.WriteLine(s);
        //
        // neoCask.Delete("75");
        //
        // var s2 = neoCask.Get("75");
        // System.Console.WriteLine(s2);
    }
}