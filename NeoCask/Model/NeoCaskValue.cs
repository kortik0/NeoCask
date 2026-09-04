using System.Text;
using NeoCask.Domain.Service;
using NeoCask.Domain.ValueObjects;

namespace NeoCask.Model;

public readonly struct NeoCaskValue
{
    public byte[] Crc32 { get; }
    public long Timestamp { get; }
    public int KeySize { get; }
    public int ValueSize { get; }
    public byte[] Key { get; }
    public byte[] Value { get; }

    public NeoCaskValue(string key, string value, bool isTombstone = false)
    {
        Key = Encoding.UTF8.GetBytes(key);
        Value = Encoding.UTF8.GetBytes(value);
        Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        KeySize = Key.Length;
        ValueSize = isTombstone ? -1 : Value.Length;
        Crc32 = GetCrcHash();
    }

    private byte[] GetCrcHash()
    {
        var crcInput = new CrcInput()
        {
            Timestamp = Timestamp,
            KeySize = KeySize,
            ValueSize = ValueSize,
            Key = Key,
            Value = Value
        };
        return CrcCalculator.GetHash(crcInput);
    }
}