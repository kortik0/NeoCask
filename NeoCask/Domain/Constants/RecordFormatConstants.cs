namespace NeoCask.Domain.Constants;

public static class RecordFormatConstants
{
    public const int CrcSize = 4;
    public const int TimestampSize = 8;
    public const int KeySizeSize = 4;
    public const int ValueSizeSize = 4;
    public const int HeaderSize = CrcSize + TimestampSize + KeySizeSize + ValueSizeSize;
}