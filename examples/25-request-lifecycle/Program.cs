// 25 - Request lifecycle: fetch purgatory (fetch.min.bytes / fetch.wait.max.ms) and leader epochs on records.
using System.Diagnostics;
using Confluent.Kafka;
using Confluent.Kafka.Admin;

const string bootstrap = "localhost:9092,localhost:9094,localhost:9096";

const string topic = "25-latency";

using (var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrap }).Build())
{
    try
    {
        await admin.DeleteTopicsAsync([topic]);

        await Task.Delay(1000);
    }
    catch (DeleteTopicsException) { }

    await admin.CreateTopicsAsync([new TopicSpecification { Name = topic, NumPartitions = 1, ReplicationFactor = 3 }]);

    await Task.Delay(500);
}

using var producer = new ProducerBuilder<Null, string>(new ProducerConfig { BootstrapServers = bootstrap, LingerMs = 0 }).Build();

Console.WriteLine("fetch.min.bytes | fetch.wait.max.ms | produce -> consume latency of ONE 20-byte record");

await Measure(fetchMinBytes: 1, fetchWaitMaxMs: 500);

await Measure(fetchMinBytes: 1_000_000, fetchWaitMaxMs: 500);

await Measure(fetchMinBytes: 1_000_000, fetchWaitMaxMs: 3000);

Console.WriteLine();

Console.WriteLine("leader epoch of each record in 12-bank-transfers partition 0 (run example 12 first):");

using (var reader = new ConsumerBuilder<string, string>(new ConsumerConfig { BootstrapServers = bootstrap, GroupId = "25-epochs" }).Build())
{
    reader.Assign(new TopicPartitionOffset("12-bank-transfers", 0, Offset.Beginning));

    while (reader.Consume(TimeSpan.FromSeconds(3)) is { } r)
    {
        Console.WriteLine($"  offset {r.Offset.Value}: leaderEpoch={r.LeaderEpoch} value={r.Message.Value}");
    }
}

async Task Measure(int fetchMinBytes, int fetchWaitMaxMs)
{
    var config = new ConsumerConfig
    {
        BootstrapServers = bootstrap,
        GroupId = "25-latency-reader",
        FetchMinBytes = fetchMinBytes,
        FetchWaitMaxMs = fetchWaitMaxMs
    };

    using var consumer = new ConsumerBuilder<Null, string>(config).Build();

    consumer.Assign(new TopicPartitionOffset(topic, 0, Offset.End));

    // start polling first, so a fetch request is already parked in the broker's purgatory
    var received = Task.Run(() => consumer.Consume(TimeSpan.FromSeconds(10)));

    await Task.Delay(1500);

    var clock = Stopwatch.StartNew();

    await producer.ProduceAsync(topic, new Message<Null, string> { Value = "order-created-00001" });

    var acked = clock.Elapsed.TotalMilliseconds;

    var record = await received;

    var latency = clock.Elapsed.TotalMilliseconds;

    Console.WriteLine($"{fetchMinBytes,15:N0} | {fetchWaitMaxMs,17} | ack {acked,4:0} ms, consumed after {latency,5:0} ms ({(record is null ? "timeout" : "ok")})");
}
