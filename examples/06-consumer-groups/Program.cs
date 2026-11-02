// 06 - Consumer groups & rebalancing: 3 consumers join and leave one group; watch partitions move.
// usage: dotnet run            -> classic protocol, cooperative-sticky assignor
//        dotnet run consumer   -> new KIP-848 protocol (broker-side assignment, Kafka 4.0+)
using System.Diagnostics;
using Confluent.Kafka;
using Confluent.Kafka.Admin;

const string bootstrap = "localhost:9092,localhost:9094,localhost:9096";

const string topic = "06-clicks";

var useNewProtocol = args.FirstOrDefault() == "consumer";

var clock = Stopwatch.StartNew();

using (var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrap }).Build())
{
    try
    {
        await admin.DeleteTopicsAsync([topic]);

        await Task.Delay(1000);
    }
    catch (DeleteTopicsException) { }

    await admin.CreateTopicsAsync([new TopicSpecification { Name = topic, NumPartitions = 6, ReplicationFactor = 3 }]);

    await Task.Delay(500);
}

Console.WriteLine($"protocol = {(useNewProtocol ? "consumer (KIP-848)" : "classic + cooperative-sticky")}");

var a = StartConsumer("A");

await Task.Delay(TimeSpan.FromSeconds(6));

var b = StartConsumer("B");

await Task.Delay(TimeSpan.FromSeconds(6));

var c = StartConsumer("C");

await Task.Delay(TimeSpan.FromSeconds(6));

Log("main", "stopping A");

a.Stop.Cancel();

await a.Loop;

await Task.Delay(TimeSpan.FromSeconds(6));

b.Stop.Cancel();

c.Stop.Cancel();

await Task.WhenAll(b.Loop, c.Loop);

(CancellationTokenSource Stop, Task Loop) StartConsumer(string name)
{
    var config = new ConsumerConfig
    {
        BootstrapServers = bootstrap,
        GroupId = useNewProtocol ? "06-clicks-group-new" : "06-clicks-group",
        AutoOffsetReset = AutoOffsetReset.Earliest
    };

    if (useNewProtocol)
    {
        config.GroupProtocol = GroupProtocol.Consumer;
    }
    else
    {
        config.PartitionAssignmentStrategy = PartitionAssignmentStrategy.CooperativeSticky;

        config.SessionTimeoutMs = 6000;

        config.HeartbeatIntervalMs = 1000;
    }

    var stop = new CancellationTokenSource();

    var loop = Task.Run(() =>
    {
        using var consumer = new ConsumerBuilder<Ignore, Ignore>(config)
            .SetPartitionsAssignedHandler((cons, added) => LogChange(name, "+ assigned", added, cons.Assignment.Concat(added)))
            .SetPartitionsRevokedHandler((cons, removed) => LogChange(name, "- revoked ", removed.Select(r => r.TopicPartition).ToList(), cons.Assignment.Except(removed.Select(r => r.TopicPartition))))
            .Build();

        Log(name, "joining group");

        consumer.Subscribe(topic);

        while (!stop.IsCancellationRequested)
        {
            // rebalance callbacks run inside Consume()
            consumer.Consume(TimeSpan.FromMilliseconds(200));
        }

        consumer.Close();

        Log(name, "left group");
    });

    return (stop, loop);
}

// cooperative rebalances also fire callbacks with empty lists; skip those
void LogChange(string who, string action, List<TopicPartition> changed, IEnumerable<TopicPartition> owned)
{
    if (changed.Count > 0)
    {
        Log(who, $"{action} [{Parts(changed)}]  now owns [{Parts(owned)}]");
    }
}

void Log(string who, string text) => Console.WriteLine($"{clock.Elapsed.TotalSeconds,5:0.0}s [{who}] {text}");

static string Parts(IEnumerable<TopicPartition> parts) => string.Join(",", parts.Select(p => p.Partition.Value).Distinct().Order());
