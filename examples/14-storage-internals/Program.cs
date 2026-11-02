// 14 - Storage internals: fill several segments, then use the time index (OffsetsForTimes).
using Confluent.Kafka;
using Confluent.Kafka.Admin;

const string bootstrap = "localhost:9092,localhost:9094,localhost:9096";

const string topic = "14-clickstream";

using (var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrap }).Build())
{
    try
    {
        await admin.DeleteTopicsAsync([topic]);

        await Task.Delay(1000);
    }
    catch (DeleteTopicsException) { }

    // 1 MiB segments (the minimum in Kafka 4.x) so 3000 x 1 KiB records span several files
    var spec = new TopicSpecification
    {
        Name = topic,
        NumPartitions = 1,
        ReplicationFactor = 3,
        Configs = new() { ["segment.bytes"] = "1048576" }
    };

    await admin.CreateTopicsAsync([spec]);

    await Task.Delay(500);
}

var baseTime = new DateTime(2026, 1, 1, 12, 0, 0, DateTimeKind.Utc);

var payload = new string('x', 1000);

using (var producer = new ProducerBuilder<Null, string>(new ProducerConfig { BootstrapServers = bootstrap, CompressionType = CompressionType.None }).Build())
{
    for (var i = 0; i < 3000; i++)
    {
        // one record per second of "event time", starting 12:00:00
        var message = new Message<Null, string> { Value = payload, Timestamp = new Timestamp(baseTime.AddSeconds(i)) };

        producer.Produce(topic, message);
    }

    producer.Flush(TimeSpan.FromSeconds(30));
}

using var consumer = new ConsumerBuilder<Null, string>(new ConsumerConfig { BootstrapServers = bootstrap, GroupId = "14-reader" }).Build();

var marks = consumer.QueryWatermarkOffsets(new TopicPartition(topic, 0), TimeSpan.FromSeconds(5));

Console.WriteLine($"written 3000 records, offsets {marks.Low.Value}..{marks.High.Value - 1}");

foreach (var at in new[] { "12:00:00", "12:10:00", "12:33:20", "12:49:59" })
{
    var time = baseTime.Date + TimeSpan.Parse(at);

    // the broker binary-searches the .timeindex, then the .index, then scans the .log
    var found = consumer.OffsetsForTimes([new TopicPartitionTimestamp(topic, 0, new Timestamp(time))], TimeSpan.FromSeconds(5));

    Console.WriteLine($"first record at or after {at} -> offset {found[0].Offset.Value}");
}
