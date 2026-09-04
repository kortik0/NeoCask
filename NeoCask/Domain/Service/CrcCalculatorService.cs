using System.Text;
using NeoCask.Domain.ValueObjects;

namespace NeoCask.Domain.Service;

public static class CrcCalculator
{
    public static byte[] GetHash(CrcInput input)
    {
        return GetCrcHash(input);
    }
    
    public static bool IsCrcCorrect(byte[] previousCrc, CrcInput input)
    {
        var currentCrcHash = GetHash(input);
        return currentCrcHash.SequenceEqual(previousCrc);
    }

    private static byte[] GetCrcHash(CrcInput input)
    {
        var bytes = GetMemoryBytes(input);
        return System.IO.Hashing.Crc32.Hash(bytes);
    }

    private static byte[] GetMemoryBytes(CrcInput input)
    {
        using var memory = new MemoryStream();
        using (var writer = new BinaryWriter(memory, Encoding.UTF8, true))
        {
            writer.Write(input.Timestamp);
            writer.Write(input.KeySize);
            writer.Write(input.ValueSize);
            writer.Write(input.Key);
            writer.Write(input.Value);
        }

        var streamBytes = memory.ToArray();
        return streamBytes;
    }
}