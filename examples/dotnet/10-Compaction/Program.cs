// 10 — Log compaction: keep only the LATEST value per key (+ tombstones delete a key)
// Run: dotnet run --project 10-Compaction      (waits ~40s for the log cleaner)
using Confluent.Kafka;

var topic = $"demo.user-profiles.{Guid.NewGuid():N}"[..30];
await Cluster.EnsureTopicAsync(topic, partitions: 1, configs: new()
{
    ["cleanup.policy"] = "compact",
    ["segment.ms"] = "1000",                  // roll segments fast (only closed segments are compacted)
    ["min.cleanable.dirty.ratio"] = "0.01",   // compact as soon as 1% is "dirty"
    ["delete.retention.ms"] = "1000",         // how long tombstones stay visible
    ["min.insync.replicas"] = "2"
});

using var producer = new ProducerBuilder<string, string?>(new ProducerConfig { BootstrapServers = Cluster.Bootstrap }).Build();

// 3 users × 5 updates each = 15 records; then delete "carol" with a tombstone (value = null)
foreach (var version in Enumerable.Range(1, 5))
    foreach (var user in new[] { "alice", "bob", "carol" })
        await producer.ProduceAsync(topic, new Message<string, string?> { Key = user, Value = $"{user}-v{version}" });
await producer.ProduceAsync(topic, new Message<string, string?> { Key = "carol", Value = null });

Console.WriteLine($"BEFORE compaction ({ReadAll().Count} records):");
Console.WriteLine("  " + string.Join(" | ", ReadAll()));

// a new record is needed to roll the active segment so the older ones become cleanable
for (var i = 0; i < 8; i++)
{
    await Task.Delay(5_000);
    await producer.ProduceAsync(topic, new Message<string, string?> { Key = "tick", Value = $"tick-{i}" });
    Console.Write(".");
}

var after = ReadAll();
Console.WriteLine($"\nAFTER compaction ({after.Count} records):");
Console.WriteLine("  " + string.Join(" | ", after));
Console.WriteLine("\nalice → v5, bob → v5, carol → gone. Offsets keep their gaps (they are never renumbered).");

List<string> ReadAll()
{
    using var c = new ConsumerBuilder<string, string?>(new ConsumerConfig
    {
        BootstrapServers = Cluster.Bootstrap,
        GroupId = $"reader-{Guid.NewGuid():N}",
        EnableAutoCommit = false
    }).Build();
    c.Assign(new TopicPartitionOffset(topic, 0, Offset.Beginning));

    var list = new List<string>();
    while (c.Consume(TimeSpan.FromSeconds(3)) is { } r)
        list.Add($"@{r.Offset.Value}:{r.Message.Key}={r.Message.Value ?? "∅"}");
    return list;
}
