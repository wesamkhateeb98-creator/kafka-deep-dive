// 11 — Replay: Kafka keeps data after it is read → rewind by offset or by time
// Run: dotnet run --project 11-SeekReplay
using Confluent.Kafka;

const string topic = "demo.clicks";
await Cluster.EnsureTopicAsync(topic, partitions: 1);
var tp = new TopicPartition(topic, 0);

// produce 10 clicks, 1 second apart, remember the time of the 6th
using (var producer = new ProducerBuilder<Null, string>(new ProducerConfig { BootstrapServers = Cluster.Bootstrap }).Build())
{
    for (var i = 1; i <= 10; i++)
    {
        await producer.ProduceAsync(topic, new Message<Null, string> { Value = $"click-{i} at {DateTime.UtcNow:HH:mm:ss}" });
        await Task.Delay(1000);
    }
}

using var consumer = new ConsumerBuilder<Null, string>(new ConsumerConfig
{
    BootstrapServers = Cluster.Bootstrap,
    GroupId = "demo-replay",
    EnableAutoCommit = false
}).Build();

var watermarks = consumer.QueryWatermarkOffsets(tp, TimeSpan.FromSeconds(5));
var high = watermarks.High;
Console.WriteLine($"partition 0: low={watermarks.Low.Value} (oldest kept), high={high.Value} (next offset to be written)");

// 1) seek by OFFSET: Assign = no group rebalance, we pick the position manually
Console.WriteLine("\n▶ replay the last 3 records (by offset):");
consumer.Assign(new TopicPartitionOffset(tp, high - 3));
Print(3);

// 2) seek by TIME: which offset was written at (now - 5s)?
var fiveSecondsAgo = new Timestamp(DateTime.UtcNow.AddSeconds(-5));
var byTime = consumer.OffsetsForTimes([new TopicPartitionTimestamp(tp, fiveSecondsAgo)], TimeSpan.FromSeconds(5));
Console.WriteLine($"\n▶ replay from 5 seconds ago → offset {byTime[0].Offset.Value}:");
consumer.Assign(byTime);
Print(10);

// 3) from the very beginning
Console.WriteLine("\n▶ Offset.Beginning:");
consumer.Assign(new TopicPartitionOffset(tp, Offset.Beginning));
Print(3);

void Print(int max)
{
    for (var i = 0; i < max && consumer.Consume(TimeSpan.FromSeconds(2)) is { } r; i++)
        Console.WriteLine($"  @{r.Offset.Value,-4} ts={r.Message.Timestamp.UtcDateTime:HH:mm:ss}  {r.Message.Value}");
}
