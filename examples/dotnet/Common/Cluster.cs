using System.Runtime.InteropServices;
using Confluent.Kafka;
using Confluent.Kafka.Admin;

/// <summary>Tiny helpers shared by all examples (the cluster from docker/docker-compose.yml).</summary>
public static class Cluster
{
    // Host default; inside Docker set KAFKA_BOOTSTRAP=kafka-1:19092,kafka-2:19092,kafka-3:19092
    public static readonly string Bootstrap =
        Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP") ?? "localhost:29092,localhost:29093,localhost:29094";

    public static IAdminClient Admin()
        => new AdminClientBuilder(new AdminClientConfig { BootstrapServers = Bootstrap }).Build();

    /// <summary>Creates the topic if missing. Default: RF=3, min.insync.replicas=2.</summary>
    public static async Task EnsureTopicAsync(string name, int partitions = 3, Dictionary<string, string>? configs = null)
    {
        using var admin = Admin();
        try
        {
            await admin.CreateTopicsAsync([new TopicSpecification
            {
                Name = name,
                NumPartitions = partitions,
                ReplicationFactor = 3,
                Configs = configs ?? new() { ["min.insync.replicas"] = "2" }
            }]);
            Console.WriteLine($"🆕 topic '{name}' created ({partitions} partitions, RF=3)");
        }
        catch (CreateTopicsException e) when (e.Results[0].Error.Code == ErrorCode.TopicAlreadyExists)
        {
            // already there — fine
        }
    }

    private static PosixSignalRegistration? _sigterm;

    /// <summary>Ctrl+C or SIGTERM (docker stop / scale-in) → cancellation token (clean consumer shutdown).</summary>
    public static CancellationToken CtrlC()
    {
        var cts = new CancellationTokenSource();
        Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };
        _sigterm = PosixSignalRegistration.Create(PosixSignal.SIGTERM, ctx => { ctx.Cancel = true; cts.Cancel(); });
        return cts.Token;
    }
}
