namespace NeoCask.Model;

public readonly struct NeoCaskHintFile(int fileId, long timestamp, int keySize, int valueSize, long offset, string key)
{
    /// <summary>
    /// This wouldn't be using in hint-file.
    /// This only using to group output list of files by them files.
    /// </summary>
    public int FileId { get; } = fileId;

    public long Timestamp { get; } = timestamp;
    public int KeySize { get; } = keySize;
    public int ValueSize { get; } = valueSize;
    public long Offset { get; } = offset;
    public string Key { get; } = key;
}