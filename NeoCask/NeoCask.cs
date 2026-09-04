using System.Collections;
using System.Collections.Concurrent;
using System.Text;
using NeoCask.Domain.Constants;
using NeoCask.Domain.Service;
using NeoCask.Domain.ValueObjects;
using NeoCask.Model;

namespace NeoCask;

public interface IKeyValueStore : IDisposable
{
    // Base operation
    public void Put(string key, string value);
    public string Get(string key);
    public void Delete(string key);
    public void Open(string directoryName);

    // Storage management
    public void Merge();
}

public class NeoCask : IKeyValueStore
{
    private bool _isDisposed;
    private readonly ConcurrentDictionary<string, EntryMetadata> _keydir = [];
    private readonly string _directory;
    private FileStream _activeFileStream;
    private int _activeFileId;
    private long _lastWrittenStartOffset;
    private readonly long _maxFileSize; //1kb
    private readonly SemaphoreSlim _semaphoreSlim = new SemaphoreSlim(1, 1);


    public NeoCask(string directory)
        : this(new NeoCaskConfiguration(directory))
    {
    }

    public NeoCask(NeoCaskConfiguration configuration)
    {
        _maxFileSize = configuration.MaxFileSize;
        _directory = configuration.DirectoryPath;
        Open(_directory);
    }

    public void Open(string directoryName)
    {
        if (!Directory.Exists(directoryName))
        {
            throw new DirectoryNotFoundException($"Directory {directoryName} not found");
        }

        var nextId = GetNextFileName(directoryName);

        if (nextId > 1)
        {
            var lastFile = Path.Combine(directoryName, $"{nextId - 1}.ncl");
            if (File.Exists(lastFile))
                RecoverCorruptedTail(lastFile);
        }

        LoadKeyDir(directoryName);

        var path = Path.Combine(directoryName, $"{nextId}.ncl");
        _activeFileId = nextId;
        _activeFileStream = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.Read);
        _lastWrittenStartOffset = _activeFileStream.Position;
    }

    private static void RecoverCorruptedTail(string path)
    {
        long lastGoodOffset = 0;
        using var fs = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.Read);
        using var reader = new BinaryReader(fs, Encoding.UTF8, true);

        try
        {
            while (fs.Position < fs.Length)
            {
                var recordPosition = fs.Position;

                var crcBytes = reader.ReadBytes(RecordFormatConstants.CrcSize);
                var timestamp = reader.ReadInt64();
                var keySize = reader.ReadInt32();
                var valueSize = reader.ReadInt32();
                var key = reader.ReadBytes(keySize);

                if (valueSize < 0)
                {
                    lastGoodOffset = recordPosition + RecordFormatConstants.HeaderSize + keySize;
                    continue;
                }

                var value = reader.ReadBytes(valueSize);

                var input = new CrcInput(timestamp, keySize, valueSize, key, value);
                if (!CrcCalculator.IsCrcCorrect(crcBytes, input))
                    throw new InvalidDataException($"CRC check failed: {path}\\{lastGoodOffset}");

                //last good offset = start position in file + structure
                lastGoodOffset = recordPosition + RecordFormatConstants.HeaderSize + keySize + valueSize;
            }
        }
        catch (InvalidDataException ex)
        {
            Console.Error.WriteLine(ex.Message);
        }

        if (lastGoodOffset < fs.Length)
        {
            fs.SetLength(lastGoodOffset);
        }
    }

    private static int GetNextFileName(string directoryName)
    {
        var files = Directory.EnumerateFiles(directoryName, "*.ncl", SearchOption.AllDirectories);

        var maxId = 0;

        foreach (var file in files)
        {
            if (!int.TryParse(Path.GetFileNameWithoutExtension(file), out var id)) continue;

            if (id > maxId)
            {
                maxId = id;
            }
        }

        return maxId + 1;
    }

    private void LoadKeyDir(string directoryName)
    {
        var dataFiles = Directory.EnumerateFiles(directoryName, "*.ncl", SearchOption.AllDirectories)
            .Select(path => new
            {
                Path = path,
                Id = int.Parse(Path.GetFileNameWithoutExtension(path))
            })
            .OrderBy(x => x.Id)
            .ToList();

        foreach (var dataFile in dataFiles)
        {
            var hintPath = Path.Combine(directoryName, $"{dataFile.Id}.hncl");

            if (File.Exists(hintPath))
            {
                var fromHintFile = GetEntryMetadataFromHintFile(dataFile.Id, hintPath);
                foreach (var entry in fromHintFile)
                    _keydir[entry.Key] = entry.Value;
                continue;
            }

            var fromDataFile = GetEntryMetadataFromDataFile(dataFile.Id, dataFile.Path);
            foreach (var entry in fromDataFile)
                _keydir[entry.Key] = entry.Value;
        }
    }

    private Dictionary<string, EntryMetadata> GetEntryMetadataFromDataFile(int fileId, string path)
    {
        Dictionary<string, EntryMetadata> entries = [];
        using var file = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        using var reader = new BinaryReader(file, Encoding.UTF8);

        while (file.Length != file.Position)
        {
            var offset = file.Position;
            _ = reader.ReadBytes(RecordFormatConstants.CrcSize);
            var timestamp = reader.ReadInt64();
            var keySize = reader.ReadInt32();
            var valueSize = reader.ReadInt32();
            var keyBytes = reader.ReadBytes(keySize);
            var key = Encoding.UTF8.GetString(keyBytes);

            if (valueSize == ValueSizeMarker.Tombstone)
            {
                entries.Remove(key);
                continue;
            }

            _ = reader.ReadBytes(valueSize);
            entries[key] = new EntryMetadata(fileId, offset, valueSize, timestamp);
        }

        return entries;
    }

    private void RotateActiveFile()
    {
        _activeFileStream.Dispose();

        _activeFileId++;
        var currentFileName = $"{_activeFileId}.ncl";
        _activeFileStream = new FileStream(Path.Combine(_directory, currentFileName), FileMode.Create,
            FileAccess.Write, FileShare.Read);
    }

    public string Get(string key)
    {
        _semaphoreSlim.Wait();
        try
        {
            if (!_keydir.TryGetValue(key, out var entry))
            {
                throw new KeyNotFoundException($"Key {key} not found");
            }

            if (entry.ValueSize == ValueSizeMarker.Tombstone)
            {
                throw new KeyNotFoundException($"Key {key} not found (it has been deleted).");
            }

            using var fs = new FileStream(Path.Combine(_directory, $"{entry.FileId}.ncl"), FileMode.Open,
                FileAccess.Read,
                FileShare.ReadWrite);
            fs.Seek(entry.Offset, SeekOrigin.Begin);

            using var reader = new BinaryReader(fs, Encoding.UTF8);
            var crc = reader.ReadBytes(RecordFormatConstants.CrcSize);
            var timestamp = reader.ReadInt64();
            var keySize = reader.ReadInt32();
            var valueSize = reader.ReadInt32();
            var keyBytes = reader.ReadBytes(keySize);
            var valueBytes = reader.ReadBytes(valueSize);

            var keyValue = Encoding.UTF8.GetString(keyBytes);
            var value = Encoding.UTF8.GetString(valueBytes);

            if (key != keyValue)
            {
                throw new KeyNotFoundException($"Key {key} is not consistent with stored key {keyValue}");
            }

            var crcInput = new CrcInput(timestamp, keySize, valueSize, keyBytes, valueBytes);

            return !CrcCalculator.IsCrcCorrect(crc, crcInput)
                ? throw new InvalidDataException($"Invalid crc")
                : $"{keyValue}:{value}";
        }
        finally
        {
            _semaphoreSlim.Release();
        }
    }

    public void Put(string key, string value)
    {
        _semaphoreSlim.Wait();

        try
        {
            PutInternal(key, value);
        }
        finally
        {
            _semaphoreSlim.Release();
        }
    }

    private void PutInternal(string key, string value, bool isTombstone = false)
    {
        var caskValue = new NeoCaskValue(key, value, isTombstone);
        var byteRecord = GetByteRecord(caskValue);
        var recordLength = byteRecord.Length;

        if (_activeFileStream.Length + recordLength > _maxFileSize)
        {
            RotateActiveFile();
        }

        _lastWrittenStartOffset = _activeFileStream.Position;
        _activeFileStream.Write(byteRecord, 0, recordLength);
        _activeFileStream.Flush(true);
        var unixTimeMilliseconds = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        _keydir[key] = new EntryMetadata(_activeFileId, _lastWrittenStartOffset, caskValue.ValueSize,
            unixTimeMilliseconds);
    }

    private static byte[] GetByteRecord(NeoCaskValue caskValue)
    {
        using var memoryStream = new MemoryStream();
        using (var binaryWriter = new BinaryWriter(memoryStream, Encoding.UTF8, true))
        {
            binaryWriter.Write(caskValue.Crc32);
            binaryWriter.Write(caskValue.Timestamp);
            binaryWriter.Write(caskValue.KeySize);
            binaryWriter.Write(caskValue.ValueSize);
            binaryWriter.Write(caskValue.Key);
            binaryWriter.Write(caskValue.Value);
        }

        var byteRecord = memoryStream.ToArray();

        return byteRecord;
    }

    public void Delete(string key)
    {
        _semaphoreSlim.Wait();
        try
        {
            if (!_keydir.TryGetValue(key, out _))
            {
                throw new KeyNotFoundException($"Key {key} not found");
            }

            PutInternal(key, string.Empty, true);
        }
        finally
        {
            _semaphoreSlim.Release();
        }
    }

    private Dictionary<string, EntryMetadata> GetEntryMetadataFromHintFile(int fileId,
        string hintFileName)
    {
        Dictionary<string, EntryMetadata> entryMetadata = [];
        using var file = new FileStream(hintFileName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        using var reader = new BinaryReader(file, Encoding.UTF8);
        while (file.Length != file.Position)
        {
            var timestamp = reader.ReadInt64();
            var keySize = reader.ReadInt32();
            var valueSize = reader.ReadInt32();
            var offset = reader.ReadInt64();
            var keyBytes = reader.ReadBytes(keySize);

            var key = Encoding.UTF8.GetString(keyBytes);

            if (valueSize == ValueSizeMarker.Tombstone)
            {
                //tombstone - move to the next
                continue;
            }

            var metadata = new EntryMetadata(fileId, offset, valueSize, timestamp);
            entryMetadata[key] = metadata;
        }

        return entryMetadata;
    }

    public void Merge()
    {
        _semaphoreSlim.Wait();
        try
        {
            var files = Directory.EnumerateFiles(_directory, "*.ncl", SearchOption.AllDirectories);
            var immutableFiles = files
                .Select(path => new
                {
                    Path = Path.GetFileNameWithoutExtension(path),
                    Id = int.Parse(Path.GetFileNameWithoutExtension(path))
                })
                .Where(x => x.Id != _activeFileId)
                .OrderBy(x => x.Id)
                .Select(x => x.Path)
                .ToList();

            var newFiles = new Dictionary<string, NeoCaskHintFile>();
            foreach (var immutableFile in immutableFiles)
            {
                var immutableFileId = Convert.ToInt32(Path.GetFileNameWithoutExtension(immutableFile));
                var path = Path.Combine(_directory, $"{immutableFile}.ncl");
                var hintPath = Path.Combine(_directory, $"{immutableFile}.hncl");

                if (Path.Exists(hintPath))
                {
                    var fromHintFile = GetEntryMetadataFromHintFile(immutableFileId, hintPath);
                    foreach (var entryMetadata in fromHintFile)
                    {
                        if (_keydir.TryGetValue(entryMetadata.Key, out var existingFromHint)
                            && existingFromHint.FileId > immutableFileId)
                            continue;

                        _keydir[entryMetadata.Key] = entryMetadata.Value;
                    }

                    continue;
                }

                using var file = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
                using var reader = new BinaryReader(file, Encoding.UTF8);
                while (file.Length != file.Position)
                {
                    var offset = file.Position;
                    var crc = reader.ReadBytes(RecordFormatConstants.CrcSize);
                    var timestamp = reader.ReadInt64();
                    var keySize = reader.ReadInt32();
                    var valueSize = reader.ReadInt32();
                    var keyBytes = reader.ReadBytes(keySize);
                    var keyValue = Encoding.UTF8.GetString(keyBytes);

                    var hasNewerVersion = _keydir.TryGetValue(keyValue, out var current)
                                          && current.FileId > immutableFileId;

                    if (valueSize == ValueSizeMarker.Tombstone)
                    {
                        if (!hasNewerVersion)
                        {
                            _keydir.Remove(keyValue, out _);
                            newFiles.Remove(keyValue);
                        }

                        continue;
                    }

                    _ = reader.ReadBytes(valueSize);

                    if (hasNewerVersion)
                        continue; 
                    
                    _keydir[keyValue] =
                        new EntryMetadata(immutableFileId, offset, valueSize, timestamp);
                    newFiles[keyValue] =
                        new NeoCaskHintFile(immutableFileId, timestamp, keySize, valueSize, offset, keyValue);
                }
            }

            var fileIds = newFiles.GroupBy(pair => pair.Value.FileId).ToList();
            foreach (var kvp in fileIds)
            {
                var fileId = kvp.Key;
                using var stream = File.Create(Path.Combine(_directory, $"{fileId}.hncl"));
                using var binaryWriter = new BinaryWriter(stream, Encoding.UTF8, true);
                foreach (var pair in kvp)
                {
                    var metadata = pair.Value;
                    var keyBytes = Encoding.UTF8.GetBytes(metadata.Key);

                    binaryWriter.Write(metadata.Timestamp);
                    binaryWriter.Write(metadata.KeySize);
                    binaryWriter.Write(metadata.ValueSize);
                    binaryWriter.Write(metadata.Offset);
                    binaryWriter.Write(keyBytes);
                }
            }
        }
        finally
        {
            _semaphoreSlim.Release();
        }
    }

    public void Dispose()
    {
        if (_isDisposed) return;
        _semaphoreSlim.Dispose();
        _activeFileStream.Dispose();
        _isDisposed = true;
    }
}