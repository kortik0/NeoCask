namespace NeoCask.Domain.ValueObjects;

public readonly struct CrcInput(long timestamp, int keySize, int valueSize, byte[] key, byte[] value)
{
    public long Timestamp { get; init; } = timestamp;
    public int KeySize { get; init; } = keySize;
    public int ValueSize { get; init; } = valueSize;
    public byte[] Key { get; init; } = key;
    public byte[] Value { get; init; } = value;
}