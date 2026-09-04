namespace NeoCask.Tests;

public abstract class NeoCaskTestBase : IDisposable
{
    protected readonly string TestDirectory;

    protected NeoCaskTestBase()
    {
        TestDirectory = Path.Combine(Path.GetTempPath(), "NeoCaskTests", Guid.NewGuid().ToString());
        Directory.CreateDirectory(TestDirectory);
    }

    public void Dispose()
    {
        try
        {
            if (Directory.Exists(TestDirectory))
                Directory.Delete(TestDirectory, true);
        }
        catch (Exception)
        {
            // ignored
        }
    }
}

public class BasicOperationsTests : NeoCaskTestBase
{
    [Fact]
    public void Put_And_Get_Should_Return_Correct_Value()
    {
        using var neoCask = new NeoCask(TestDirectory);
        var key = "myKey";
        var value = "myValue";
        var expectedResult = $"{key}:{value}";

        neoCask.Put(key, value);
        var actualResult = neoCask.Get(key);

        Assert.Equal(expectedResult, actualResult);
    }

    [Fact]
    public void Update_Existing_Key_Should_Return_Latest_Value()
    {
        using var neoCask = new NeoCask(TestDirectory);
        var key = "updateKey";
        var initialValue = "initial";
        var updatedValue = "updated";
        var expectedResult = $"{key}:{updatedValue}";

        neoCask.Put(key, initialValue);
        neoCask.Put(key, updatedValue);
        var actualResult = neoCask.Get(key);

        Assert.Equal(expectedResult, actualResult);
    }

    [Fact]
    public void Delete_Existing_Key_Should_Make_It_Inaccessible()
    {
        using var neoCask = new NeoCask(TestDirectory);
        var key = "keyToDelete";
        var value = "someValue";

        neoCask.Put(key, value);
        neoCask.Delete(key);

        Assert.Throws<KeyNotFoundException>(() => neoCask.Get(key));
    }

    [Fact]
    public void Get_Non_Existent_Key_Should_Throw_KeyNotFoundException()
    {
        using var neoCask = new NeoCask(TestDirectory);

        Assert.Throws<KeyNotFoundException>(() => neoCask.Get("nonExistentKey"));
    }

    [Fact]
    public void Delete_Non_Existent_Key_Should_Throw_KeyNotFoundException()
    {
        using var neoCask = new NeoCask(TestDirectory);

        Assert.Throws<KeyNotFoundException>(() => neoCask.Delete("nonExistentKey"));
    }

    [Theory]
    [InlineData("key with spaces", "value with spaces")]
    [InlineData("!@#$%^", "(*)-=+")]
    [InlineData("ключ_на_кириллице", "значение_на_кириллице")]
    [InlineData("key_with_empty_value", "")]
    public void Put_And_Get_With_Special_Characters_Or_Empty_Value(string key, string value)
    {
        using var neoCask = new NeoCask(TestDirectory);
        var expectedResult = $"{key}:{value}";

        neoCask.Put(key, value);
        var actualResult = neoCask.Get(key);

        Assert.Equal(expectedResult, actualResult);
    }
}

public class PersistenceAndRecoveryTests : NeoCaskTestBase
{
    [Fact]
    public void Data_Should_Persist_After_Reopening_Database_Without_Merge()
    {
        var key = "persistKey";
        var value = "persistValue";
        var expectedResult = $"{key}:{value}";

        using (var neoCask1 = new NeoCask(TestDirectory))
        {
            neoCask1.Put(key, value);
        }

        using var neoCask2 = new NeoCask(TestDirectory);
        var result = neoCask2.Get(key);

        Assert.Equal(expectedResult, result);
    }

    [Fact]
    public void Data_Should_Persist_Across_Multiple_Reopenings_Without_Merge()
    {
        var keys = new[] { "k1", "k2", "k3" };

        using (var neoCask1 = new NeoCask(TestDirectory))
        {
            neoCask1.Put(keys[0], "v1");
        }

        using (var neoCask2 = new NeoCask(TestDirectory))
        {
            Assert.Equal($"{keys[0]}:v1", neoCask2.Get(keys[0]));
            neoCask2.Put(keys[1], "v2");
        }

        using (var neoCask3 = new NeoCask(TestDirectory))
        {
            Assert.Equal($"{keys[0]}:v1", neoCask3.Get(keys[0]));
            Assert.Equal($"{keys[1]}:v2", neoCask3.Get(keys[1]));
            neoCask3.Put(keys[2], "v3");
        }

        using var neoCask4 = new NeoCask(TestDirectory);
        Assert.Equal($"{keys[0]}:v1", neoCask4.Get(keys[0]));
        Assert.Equal($"{keys[1]}:v2", neoCask4.Get(keys[1]));
        Assert.Equal($"{keys[2]}:v3", neoCask4.Get(keys[2]));
    }
}

public class MergeOperationTests : NeoCaskTestBase
{
    [Fact]
    public void Merge_Should_Preserve_Latest_Value_Of_A_Key()
    {
        var key = "mergeKey";
        var value1 = "value1";
        var value2 = "latest_value";

        using var neoCask = new NeoCask(TestDirectory);

        neoCask.Put("dummy_key_1", new string('a', 600));
        neoCask.Put(key, value1);

        neoCask.Put("dummy_key_2", new string('b', 600));
        neoCask.Put(key, value2);

        neoCask.Merge();

        Assert.Equal($"{key}:{value2}", neoCask.Get(key));
    }

    [Fact]
    public void Merge_Should_Remove_Deleted_Keys_Permanently()
    {
        var keyToKeep = "keyToKeep";
        var keyToDelete = "keyToDelete";

        using (var neoCask = new NeoCask(TestDirectory))
        {
            neoCask.Put(keyToKeep, "value1");
            neoCask.Put(keyToDelete, "value2");
            neoCask.Delete(keyToDelete);

            var filesBeforeMerge = Directory.GetFiles(TestDirectory, "*.ncl").Length;
            Assert.True(filesBeforeMerge > 0);

            neoCask.Merge();

            Assert.Equal($"{keyToKeep}:value1", neoCask.Get(keyToKeep));
            Assert.Throws<KeyNotFoundException>(() => neoCask.Get(keyToDelete));
        }

        using var neoCaskAfterRestart = new NeoCask(TestDirectory);
        Assert.Throws<KeyNotFoundException>(() => neoCaskAfterRestart.Get(keyToDelete));
    }

    [Fact]
    public void Merge_Should_Not_Corrupt_Active_File()
    {
        // Заполняем достаточно данных, чтобы гарантировать ротацию файла,
        // затем мержим и продолжаем писать в новый активный файл.
        using var neoCask = new NeoCask(TestDirectory);
        var bigValue = new string('a', 600);

        neoCask.Put("old_key_1", bigValue);
        neoCask.Put("old_key_2", bigValue);

        neoCask.Merge();

        neoCask.Put("new_key", "new_value");

        Assert.Equal("old_key_1:" + bigValue, neoCask.Get("old_key_1"));
        Assert.Equal("new_key:new_value", neoCask.Get("new_key"));
    }
}

public class FileManagementTests : NeoCaskTestBase
{
    [Fact]
    public void Active_File_Should_Rotate_When_Size_Exceeds_Threshold()
    {
        const int maxFileSize = 1024; 
        var value = new string('a', 300);
        
        using var neoCask = new NeoCask(TestDirectory);
        
        var initialFileCount = Directory.GetFiles(TestDirectory, "*.ncl").Length;

        int writes = 0;
        while(Directory.GetFiles(TestDirectory, "*.ncl").Length == initialFileCount)
        {
            neoCask.Put($"rotateKey_{writes++}", value);
            if (writes > (maxFileSize / value.Length) + 5) 
            {
                Assert.Fail("File did not rotate after a reasonable number of writes.");
            }
        }
        
        var finalFileCount = Directory.GetFiles(TestDirectory, "*.ncl").Length;

        Assert.True(finalFileCount > initialFileCount, "File count should have increased after rotation.");
    }
}