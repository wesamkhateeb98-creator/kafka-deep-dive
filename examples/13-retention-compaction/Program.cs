// 13 - Retention & log compaction: a compacted topic keeps only the latest value per key.
using Confluent.Kafka;
using Confluent.Kafka.Admin;

const string bootstrap = "localhost:9092,localhost:9094,localhost:9096";

const string topic = "13-customer-addresses";

using (var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrap }).Build())
{
    try
    {
        await admin.DeleteTopicsAsync([topic]);

        await Task.Delay(1000);
    }
    catch (DeleteTopicsException) { }

    // tiny thresholds so the cleaner runs within seconds (never use these values in production)
    var spec = new TopicSpecification
    {
        Name = topic,
        NumPartitions = 1,
        ReplicationFactor = 3,
        Configs = new()
        {
            ["cleanup.policy"] = "compact",
            ["segment.ms"] = "1000",
            ["min.cleanable.dirty.ratio"] = "0.01",
            ["delete.retention.ms"] = "1000"
        }
    };

    await admin.CreateTopicsAsync([spec]);

    await Task.Delay(500);
}

using var producer = new ProducerBuilder<string, string?>(new ProducerConfig { BootstrapServers = bootstrap }).Build();

(string Key, string? Value)[] updates =
[
    ("cust-1", "Berlin"),
    ("cust-2", "Paris"),
    ("cust-1", "Munich"),
    ("cust-3", "Rome"),
    ("cust-2", "Lyon"),
    ("cust-1", "Hamburg"),
    ("cust-3", null)
];

foreach (var (key, value) in updates)
{
    // a null value is a tombstone: "delete this key"
    await producer.ProduceAsync(topic, new Message<string, string?> { Key = key, Value = value });
}

Console.WriteLine("before compaction:");

Print(ReadAll());

// the active segment is never compacted: wait for segment.ms, then write once more to roll it
for (var i = 0; i < 4; i++)
{
    await Task.Delay(TimeSpan.FromSeconds(3));

    await producer.ProduceAsync(topic, new Message<string, string?> { Key = "roll", Value = $"tick-{i}" });
}

Console.WriteLine("waiting for the log cleaner ...");

await Task.Delay(TimeSpan.FromSeconds(15));

Console.WriteLine("after compaction:");

Print(ReadAll().Where(r => r.Message.Key != "roll"));

List<ConsumeResult<string, string?>> ReadAll()
{
    using var consumer = new ConsumerBuilder<string, string?>(new ConsumerConfig { BootstrapServers = bootstrap, GroupId = "13-reader" }).Build();

    consumer.Assign(new TopicPartitionOffset(topic, 0, Offset.Beginning));

    var records = new List<ConsumeResult<string, string?>>();

    while (consumer.Consume(TimeSpan.FromSeconds(2)) is { } record)
    {
        records.Add(record);
    }

    return records;
}

static void Print(IEnumerable<ConsumeResult<string, string?>> records)
{
    foreach (var r in records)
    {
        Console.WriteLine($"  offset {r.Offset.Value}: {r.Message.Key} = {r.Message.Value ?? "<tombstone>"}");
    }
}
