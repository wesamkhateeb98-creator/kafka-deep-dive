// 29 - Streams deep dive: tumbling windows (event time) and a stream-table join, with Streamiz.
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Microsoft.Extensions.Logging;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;

const string bootstrap = "localhost:9092,localhost:9094,localhost:9096";

string[] topics = ["29-clicks", "29-orders", "29-customers", "29-clicks-per-10s", "29-enriched-orders"];

using (var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrap }).Build())
{
    try
    {
        await admin.DeleteTopicsAsync(topics);

        await Task.Delay(1500);
    }
    catch (DeleteTopicsException) { }

    // joins need co-partitioned inputs: same partition count, same key
    await admin.CreateTopicsAsync(topics.Select(t => new TopicSpecification { Name = t, NumPartitions = 1, ReplicationFactor = 3 }));

    await Task.Delay(500);
}

var builder = new StreamBuilder();

// A) clicks per user per 10-second tumbling window (event time = record timestamp)
builder.Stream<string, string>("29-clicks")
    .GroupByKey()
    .WindowedBy(TumblingWindowOptions.Of(TimeSpan.FromSeconds(10)))
    .Count(InMemoryWindows.As<string, long>("clicks-window-store"))
    .ToStream()
    .Map((window, count, _) => KeyValuePair.Create(
        $"{window.Key}@{DateTimeOffset.FromUnixTimeMilliseconds(window.Window.StartMs):HH:mm:ss}-{DateTimeOffset.FromUnixTimeMilliseconds(window.Window.EndMs):HH:mm:ss}",
        count.ToString()))
    .To("29-clicks-per-10s");

// B) enrich each order with the customer's current tier (KStream-KTable join)
var customers = builder.Table("29-customers", InMemory.As<string, string>("customers-store"));

builder.Stream<string, string>("29-orders")
    .Join(customers, (order, tier) => $"{order} (tier={tier})")
    .To("29-enriched-orders");

var config = new StreamConfig<StringSerDes, StringSerDes>
{
    ApplicationId = $"29-streams-{DateTime.UtcNow:HHmmss}",
    BootstrapServers = bootstrap,
    AutoOffsetReset = AutoOffsetReset.Earliest,
    CommitIntervalMs = 500,
    ReplicationFactor = 3,
    // Streamiz logs every config value at Info; keep only warnings
    Logger = LoggerFactory.Create(b => b.SetMinimumLevel(LogLevel.Warning))
};

var stream = new KafkaStream(builder.Build(), config);

await stream.StartAsync();

using (var producer = new ProducerBuilder<string, string>(new ProducerConfig { BootstrapServers = bootstrap }).Build())
{
    var t0 = new DateTime(2026, 1, 1, 12, 0, 0, DateTimeKind.Utc);

    // event times: alice at :01 :04 :09 :12, bob at :03 :15
    (string User, int Second)[] clicks = [("alice", 1), ("bob", 3), ("alice", 4), ("alice", 9), ("alice", 12), ("bob", 15)];

    foreach (var (user, second) in clicks)
    {
        await producer.ProduceAsync("29-clicks", new Message<string, string> { Key = user, Value = "click", Timestamp = new Timestamp(t0.AddSeconds(second)) });
    }

    // table first (customer tiers), then orders; carol has no customer row -> inner join drops her order
    await producer.ProduceAsync("29-customers", new Message<string, string> { Key = "alice", Value = "gold" });

    await producer.ProduceAsync("29-customers", new Message<string, string> { Key = "bob", Value = "silver" });

    await Task.Delay(3000);

    await producer.ProduceAsync("29-orders", new Message<string, string> { Key = "alice", Value = "order-1 120 EUR" });

    await producer.ProduceAsync("29-orders", new Message<string, string> { Key = "carol", Value = "order-2 40 EUR" });

    await producer.ProduceAsync("29-customers", new Message<string, string> { Key = "bob", Value = "gold" });

    await Task.Delay(2000);

    await producer.ProduceAsync("29-orders", new Message<string, string> { Key = "bob", Value = "order-3 75 EUR" });
}

await Task.Delay(TimeSpan.FromSeconds(8));

stream.Dispose();

foreach (var topic in new[] { "29-clicks-per-10s", "29-enriched-orders" })
{
    Console.WriteLine($"== {topic}");

    using var consumer = new ConsumerBuilder<string, string>(new ConsumerConfig { BootstrapServers = bootstrap, GroupId = "29-reader" }).Build();

    consumer.Assign(new TopicPartitionOffset(topic, 0, Offset.Beginning));

    while (consumer.Consume(TimeSpan.FromSeconds(3)) is { } r)
    {
        Console.WriteLine($"  {r.Message.Key,-24} -> {r.Message.Value}");
    }
}
