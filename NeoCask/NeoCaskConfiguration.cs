namespace NeoCask;

public readonly struct NeoCaskConfiguration
{
    public NeoCaskConfiguration(string directoryPath)
    {
        DirectoryPath = directoryPath;
        if (string.IsNullOrEmpty(DirectoryPath) || !Directory.Exists(DirectoryPath))
            throw new ArgumentException("NeoCask configuration directory doesn't exist");
        if (MaxFileSize <= 0)
            throw new ArgumentException("NeoCask configuration max file size is negative");
    }

    public string DirectoryPath { get; }
    public long MaxFileSize { get; } = 1 * 1024;
}