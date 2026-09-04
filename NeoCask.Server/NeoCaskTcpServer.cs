using System.Net.Sockets;
using System.Text;

namespace NeoCask.Server;

public class NeoCaskTcpServer
{
    private CancellationTokenSource _cts;
    private IKeyValueStore _store;
    private TcpListener _listener;
    private Task _listenTask;
    private readonly int _port;

    public NeoCaskTcpServer(string directory, int port = 9736)
    {
        _store = new NeoCask(directory);
        _port = port;
    }

    public async Task StartAsync()
    {
        _cts = new CancellationTokenSource();
        _listener = TcpListener.Create(_port);
        _listener.Start();

        Console.WriteLine($"Listening on port {_port}");

        _listenTask = AcceptClientAsync(_cts.Token);
    }

    private async Task AcceptClientsAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                var client = await AcceptClientAsync(ct);
                if (client != null)
                {
                    _ = Task.Run(() => HandleClientAsync(client, ct), ct);
                }
            }
            catch (ObjectDisposedException)
            {
                break;
            }
        }
    }

    private async Task HandleClientAsync(TcpClient client, CancellationToken ct)
    {
        using (client)
        {
            var buffer = new byte[4096];
            var stream = client.GetStream();
            var messageBuilder = new StringBuilder();

            try
            {
                while (!ct.IsCancellationRequested && client.Connected)
                {
                    var bytesReader = await stream.ReadAsync(buffer, ct);
                    if (bytesReader == 0)
                        break;

                    messageBuilder.Append(Encoding.UTF8.GetString(buffer, 0, bytesReader));

                    string messages = messageBuilder.ToString();
                    int lastIndex = 0;

                    while (true)
                    {
                        int index = messages.IndexOf("/r/n", lastIndex, StringComparison.Ordinal);
                        if (index == -1)
                            break;

                        string command = messages.Substring(lastIndex, index - lastIndex);
                        await ProcessCommandAsync(command, stream);
                        lastIndex = index + 2;
                    }

                    if (lastIndex > 0)
                    {
                        messageBuilder.Clear();
                        if (lastIndex < messages.Length)
                        {
                            messageBuilder.Append(messages.Substring(lastIndex));
                        }
                    }
                }
            }
            catch (IOException)
            {
                // Client disconnected
            }
            catch (ObjectDisposedException)
            {
                // Socket closed
            }
        }
    }

    private async Task ProcessCommandAsync(string command, NetworkStream stream)
    {
        var parts = command.Split(' ', StringSplitOptions.RemoveEmptyEntries);
        if (parts.Length == 0)
            return;

        var response = parts[0].ToUpper() switch
        {
            "GET" => ProcessGet(parts),
            "PUT" => ProcessPut(parts),
            "DELETE" => ProcessDelete(parts),
            "MERGE" => ProcessMerge(parts),
            "PING" => "+PONG\r\n",
            _ => "-ERROR Unknown command\r\n"
        };

        var responseBytes = Encoding.UTF8.GetBytes(response);
        await stream.WriteAsync(responseBytes);
        await stream.FlushAsync();
    }

    private string ProcessGet(string[] parts)
    {
        if (parts.Length != 2)
            return "-ERROR GET requires exactly one argument\r\n";

        try
        {
            var value = _store.Get(parts[1]);
            return $"+OK {value}\r\n";
        }
        catch (KeyNotFoundException)
        {
            return "-ERROR Key not found\r\n";
        }
        catch (Exception ex)
        {
            return $"-ERROR {ex.Message}\r\n";
        }
    }

    private string ProcessPut(string[] parts)
    {
        if (parts.Length < 3)
            return "-ERROR PUT requires at least two arguments\r\n";

        try
        {
            var key = parts[1];
            var value = string.Join(" ", parts.Skip(2));
            _store.Put(key, value);
            return "+OK\r\n";
        }
        catch (Exception ex)
        {
            return $"-ERROR {ex.Message}\r\n";
        }
    }

    private string ProcessDelete(string[] parts)
    {
        if (parts.Length != 2)
            return "-ERROR DELETE requires exactly one argument\r\n";

        try
        {
            _store.Delete(parts[1]);
            return "+OK\r\n";
        }
        catch (KeyNotFoundException)
        {
            return "-ERROR Key not found\r\n";
        }
        catch (Exception ex)
        {
            return $"-ERROR {ex.Message}\r\n";
        }
    }

    private string ProcessMerge(string[] parts)
    {
        if (parts.Length != 1)
            return "-ERROR MERGE takes no arguments\r\n";

        try
        {
            _store.Merge();
            return "+OK\r\n";
        }
        catch (Exception ex)
        {
            return $"-ERROR {ex.Message}\r\n";
        }
    }

    public async Task StopAsync()
    {
        _cts.Cancel();
        _listener?.Stop();

        if (_listenTask != null)
            await _listenTask;

        Console.WriteLine("NeoCask server stopped");
    }

    public void Dispose()
    {
        _cts?.Cancel();
        _listener?.Stop();
        _store?.Dispose();
        _cts?.Dispose();
    }

    private async Task<TcpClient?> AcceptClientAsync(CancellationToken ct)
    {
        try
        {
            await using (ct.Register(() => _listener.Stop()))
            {
                var tcpClientTask = _listener.AcceptTcpClientAsync(ct).AsTask();
                var tcs = new TaskCompletionSource<bool>();
                await using (ct.Register(() => tcs.TrySetCanceled()))
                {
                    var completedTask = await Task.WhenAny(tcpClientTask, tcs.Task);
                    if (completedTask == tcpClientTask)
                        return await tcpClientTask;
                }
            }
        }
        catch (ObjectDisposedException) when (ct.IsCancellationRequested)
        {
        }

        return null;
    }
}