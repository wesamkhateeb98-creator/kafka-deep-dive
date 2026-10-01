// 05 — Keys, partitioners and ordering
// Run: dotnet run --project 05-KeysOrdering
// Shows:
//   1) same key → same partition, offsets strictly increasing → per-key ordering
//   2) Murmur2 (Java default) vs ConsistentRandom (librdkafka default) map the SAME key to DIFFERENT partitions
//   3) null key → spread over partitions (no ordering guarantee)
using Confluent.Kafka;

const string topic = "demo.keys";
await Cluster.EnsureTopicAsync(topic, partitions: 6);

async Task<List<(string? Key, int Partition, long Offset)>> SendAsync(Partitioner partitioner, IEnumerable<string?> keys)
{
    using var producer = new ProducerBuilder<string?, string>(new ProducerConfig
    {
        BootstrapServers = Cluster.Bootstrap,
        Partitioner = partitioner,
        EnableIdempotence = true
    }).Build();

    var results = new List<(string?, int, long)>();
    foreach (var key in keys)
    {
        var r = await producer.ProduceAsync(topic, new Message<string?, string> { Key = key, Value = "x" });
        results.Add((key, r.Partition.Value, r.Offset.Value));
    }
    return results;
}

// 1) ordering per key
var keys = new[] { "alice", "bob", "carol", "alice", "bob", "alice", "dave", "alice" };
Console.WriteLine("=== Murmur2Random (use this: compatible with Java/Go/Python clients) ===");
foreach (var (key, partition, offset) in await SendAsync(Partitioner.Murmur2Random, keys))
    Console.WriteLine($"{key,-6} → p{partition} @{offset}");

// 2) partitioner mismatch
Console.WriteLine("\n=== key → partition: Murmur2 vs ConsistentRandom ===");
var distinct = new[] { "alice", "bob", "carol", "dave", "erin", "frank" };
var murmur = await SendAsync(Partitioner.Murmur2Random, distinct);
var crc = await SendAsync(Partitioner.ConsistentRandom, distinct);
for (var i = 0; i < distinct.Length; i++)
{
    var mismatch = murmur[i].Partition != crc[i].Partition ? "  ⚠ different!" : "";
    Console.WriteLine($"{distinct[i],-6} murmur2=p{murmur[i].Partition}  crc32=p{crc[i].Partition}{mismatch}");
}

// 3) null key
Console.WriteLine("\n=== null key (20 sends) → partitions used ===");
var nulls = await SendAsync(Partitioner.Murmur2Random, Enumerable.Repeat<string?>(null, 20));
Console.WriteLine(string.Join(" ", nulls.Select(n => $"p{n.Partition}")));
