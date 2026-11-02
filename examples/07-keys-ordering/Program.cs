// 07 - Keys & ordering: same key -> same partition -> ordered. Compare .NET default vs Java-compatible partitioner.
using Confluent.Kafka;
using Confluent.Kafka.Admin;

const string bootstrap = "localhost:9092,localhost:9094,localhost:9096";

const string topic = "07-account-events";

using (var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrap }).Build())
{
    try
    {
        await admin.DeleteTopicsAsync([topic]);

        await Task.Delay(1000);
    }
    catch (DeleteTopicsException) { }

    await admin.CreateTopicsAsync([new TopicSpecification { Name = topic, NumPartitions = 3, ReplicationFactor = 3 }]);

    await Task.Delay(500);
}

string[] keys = ["acc-1", "acc-2", "acc-3", "acc-4", "acc-5"];

string[] steps = ["opened", "deposit", "withdraw"];

Console.WriteLine("key    | .NET default (consistent_random, CRC32) | Java default (murmur2)");

Console.WriteLine("-------+-----------------------------------------+----------------------");

var native = await PartitionsFor(Partitioner.ConsistentRandom);

var java = await PartitionsFor(Partitioner.Murmur2Random);

foreach (var key in keys)
{
    Console.WriteLine($"{key,-6} | {native[key],-39} | {java[key]}");
}

// read partition by partition: inside one partition, events of a key keep their order
using var consumer = new ConsumerBuilder<string, string>(new ConsumerConfig { BootstrapServers = bootstrap, GroupId = "07-reader" }).Build();

Console.WriteLine();

for (var p = 0; p < 3; p++)
{
    consumer.Assign(new TopicPartitionOffset(topic, p, Offset.Beginning));

    var lines = new List<string>();

    while (consumer.Consume(TimeSpan.FromSeconds(2)) is { } record)
    {
        if (record.Message.Headers.TryGetLastBytes("partitioner", out var h) && h[0] == 'm')
        {
            lines.Add($"{record.Message.Key}:{record.Message.Value}");
        }
    }

    Console.WriteLine($"partition {p} (murmur2 run): {string.Join("  ", lines)}");
}

async Task<Dictionary<string, string>> PartitionsFor(Partitioner partitioner)
{
    var config = new ProducerConfig { BootstrapServers = bootstrap, Partitioner = partitioner };

    using var producer = new ProducerBuilder<string, string>(config).Build();

    var seen = new Dictionary<string, HashSet<int>>();

    foreach (var step in steps)
    {
        foreach (var key in keys)
        {
            var headers = new Headers { { "partitioner", [(byte)(partitioner == Partitioner.Murmur2Random ? 'm' : 'c')] } };

            var result = await producer.ProduceAsync(topic, new Message<string, string> { Key = key, Value = step, Headers = headers });

            seen.TryAdd(key, []);

            seen[key].Add(result.Partition.Value);
        }
    }

    // a key that always lands on one partition prints a single number
    return seen.ToDictionary(kv => kv.Key, kv => $"partition {string.Join(",", kv.Value)}");
}
