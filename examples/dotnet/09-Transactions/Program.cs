// 09 — Transactions & Exactly-Once (EOS)
// Run: dotnet run --project 09-Transactions
// Part A: committed vs aborted transactions seen by read_committed vs read_uncommitted
// Part B: consume → transform → produce, with the consumer offset committed INSIDE the transaction
using Confluent.Kafka;

const string input = "demo.tx.input";
const string output = "demo.tx.output";
await Cluster.EnsureTopicAsync(input, partitions: 1);
await Cluster.EnsureTopicAsync(output, partitions: 1);
var runId = Guid.NewGuid().ToString("N")[..6];

// ── Part A ───────────────────────────────────────────────
using var tx = new ProducerBuilder<string, string>(new ProducerConfig
{
    BootstrapServers = Cluster.Bootstrap,
    TransactionalId = "demo-tx-producer"   // stable id → zombie fencing across restarts (epoch++)
}).Build();
tx.InitTransactions(TimeSpan.FromSeconds(10));

var start = Watermark(output);

tx.BeginTransaction();
for (var i = 1; i <= 3; i++) await tx.ProduceAsync(output, new Message<string, string> { Key = runId, Value = $"committed-{i}" });
tx.CommitTransaction();

tx.BeginTransaction();
for (var i = 1; i <= 3; i++) await tx.ProduceAsync(output, new Message<string, string> { Key = runId, Value = $"aborted-{i}" });
tx.AbortTransaction();                     // records stay in the log, marked as aborted

Console.WriteLine($"read_uncommitted sees: {string.Join(", ", ReadFrom(output, start, IsolationLevel.ReadUncommitted))}");
Console.WriteLine($"read_committed   sees: {string.Join(", ", ReadFrom(output, start, IsolationLevel.ReadCommitted))}");

// ── Part B: exactly-once consume-transform-produce ───────
using (var seed = new ProducerBuilder<string, string>(new ProducerConfig { BootstrapServers = Cluster.Bootstrap }).Build())
    for (var i = 1; i <= 3; i++) await seed.ProduceAsync(input, new Message<string, string> { Key = runId, Value = $"{i * 100}" });

using var consumer = new ConsumerBuilder<string, string>(new ConsumerConfig
{
    BootstrapServers = Cluster.Bootstrap,
    GroupId = $"demo-eos-{runId}",
    AutoOffsetReset = AutoOffsetReset.Earliest,
    EnableAutoCommit = false,              // offsets go through the transaction instead
    IsolationLevel = IsolationLevel.ReadCommitted
}).Build();
consumer.Assign(new TopicPartitionOffset(input, 0, Watermark(input) - 3));

for (var i = 0; i < 3; i++)
{
    var r = consumer.Consume(TimeSpan.FromSeconds(10))!;
    var converted = $"{decimal.Parse(r.Message.Value) * 0.92m} EUR";

    tx.BeginTransaction();
    await tx.ProduceAsync(output, new Message<string, string> { Key = r.Message.Key, Value = converted });
    tx.SendOffsetsToTransaction(                                   // output write + input offset: atomic
        [new TopicPartitionOffset(r.TopicPartition, r.Offset + 1)],
        consumer.ConsumerGroupMetadata,
        TimeSpan.FromSeconds(10));
    tx.CommitTransaction();

    Console.WriteLine($"EOS: {r.Message.Value} USD → {converted}  (input offset {r.Offset.Value} committed in the same tx)");
}

static long Watermark(string topic)
{
    using var c = new ConsumerBuilder<Ignore, Ignore>(new ConsumerConfig { BootstrapServers = Cluster.Bootstrap, GroupId = "wm" }).Build();
    return c.QueryWatermarkOffsets(new TopicPartition(topic, 0), TimeSpan.FromSeconds(5)).High;
}

static List<string> ReadFrom(string topic, long offset, IsolationLevel isolation)
{
    using var c = new ConsumerBuilder<string, string>(new ConsumerConfig
    {
        BootstrapServers = Cluster.Bootstrap,
        GroupId = $"reader-{Guid.NewGuid():N}",
        IsolationLevel = isolation,
        EnableAutoCommit = false
    }).Build();
    c.Assign(new TopicPartitionOffset(topic, 0, offset));

    var values = new List<string>();
    while (c.Consume(TimeSpan.FromSeconds(3)) is { } r) values.Add(r.Message.Value);
    return values;
}
