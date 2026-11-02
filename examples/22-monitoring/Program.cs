// 22 - Monitoring: consumer lag (the #1 Kafka metric) and client statistics.
using System.Text.Json;
using Confluent.Kafka;
using Confluent.Kafka.Admin;

const string bootstrap = "localhost:9092,localhost:9094,localhost:9096";

const string topic = "22-emails";

const string group = "22-email-sender";

using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrap }).Build();

try
{
    await admin.DeleteTopicsAsync([topic]);

    await Task.Delay(1000);

    await admin.DeleteGroupsAsync([group]);
}
catch (KafkaException) { }

await admin.CreateTopicsAsync([new TopicSpecification { Name = topic, NumPartitions = 3, ReplicationFactor = 3 }]);

await Task.Delay(500);

string? lastStats = null;

var producerConfig = new ProducerConfig { BootstrapServers = bootstrap, StatisticsIntervalMs = 1000 };

using (var producer = new ProducerBuilder<string, string>(producerConfig).SetStatisticsHandler((_, json) => lastStats = json).Build())
{
    for (var i = 0; i < 300; i++)
    {
        producer.Produce(topic, new Message<string, string> { Key = $"user-{i}", Value = $"welcome email {i}" });
    }

    producer.Flush(TimeSpan.FromSeconds(10));

    // wait for one statistics callback after the flush
    await Task.Delay(1500);
}

var stats = JsonDocument.Parse(lastStats!).RootElement;

Console.WriteLine($"producer stats: txmsgs={stats.GetProperty("txmsgs")} txbytes={stats.GetProperty("txmsg_bytes")} queued={stats.GetProperty("msg_cnt")}");

foreach (var broker in stats.GetProperty("brokers").EnumerateObject().Where(b => b.Value.GetProperty("nodeid").GetInt32() >= 0))
{
    var rtt = broker.Value.GetProperty("rtt");

    Console.WriteLine($"  broker {broker.Value.GetProperty("nodeid")}: requests={broker.Value.GetProperty("tx")} rtt avg={rtt.GetProperty("avg").GetInt64() / 1000.0:0.0} ms p99={rtt.GetProperty("p99").GetInt64() / 1000.0:0.0} ms");
}

// a slow consumer: handles 100 of 300 emails, commits, stops
var consumerConfig = new ConsumerConfig
{
    BootstrapServers = bootstrap,
    GroupId = group,
    AutoOffsetReset = AutoOffsetReset.Earliest,
    EnableAutoCommit = false,
    EnableAutoOffsetStore = false
};

using (var consumer = new ConsumerBuilder<string, string>(consumerConfig).Build())
{
    consumer.Subscribe(topic);

    for (var handled = 0; handled < 100;)
    {
        var record = consumer.Consume(TimeSpan.FromSeconds(5));

        if (record is null)
        {
            continue;
        }

        consumer.StoreOffset(record);

        handled++;
    }

    consumer.Commit();

    consumer.Close();
}

// lag = log end offset (high watermark) - committed offset, per partition
// ask for every partition explicitly: partitions the group never committed count from offset 0
var partitions = Enumerable.Range(0, 3).Select(p => new TopicPartition(topic, p)).ToList();

var committed = await admin.ListConsumerGroupOffsetsAsync([new ConsumerGroupTopicPartitions(group, partitions)]);

using var probe = new ConsumerBuilder<Ignore, Ignore>(new ConsumerConfig { BootstrapServers = bootstrap, GroupId = "22-probe" }).Build();

Console.WriteLine();

Console.WriteLine("partition | end offset | committed | lag");

long total = 0;

foreach (var tpo in committed[0].Partitions.OrderBy(p => p.Partition.Value))
{
    var end = probe.QueryWatermarkOffsets(tpo.TopicPartition, TimeSpan.FromSeconds(5)).High.Value;

    var done = tpo.Offset.Value < 0 ? 0 : tpo.Offset.Value;

    total += end - done;

    Console.WriteLine($"{tpo.Partition.Value,9} | {end,10} | {done,9} | {end - done}");
}

Console.WriteLine($"total lag for group {group} = {total}");
