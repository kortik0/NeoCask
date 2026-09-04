namespace NeoCask.Model;

public readonly struct EntryMetadata(int fileId, long offset, int valueSize, long timestamp)
{
    /// <summary>
    /// file_id
    /// </summary>
    public int FileId { get; } = fileId;

    /// <summary>
    /// value_position
    /// </summary>
    public long Offset { get; } = offset;

    /// <summary>
    /// value_size
    /// </summary>
    public int ValueSize { get; } = valueSize;

    /// <summary>
    /// timestamp
    /// </summary>
    public long Timestamp { get; } = timestamp;
}