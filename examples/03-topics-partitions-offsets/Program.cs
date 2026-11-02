// 03 - Topics, partitions, offsets: create a topic, write to each partition, read the offsets back.
using Confluent.Kafka;
using Confluent.Kafka.Admin;

const string bootstrap = "localhost:9092,localhost:9094,localhost:9096";

const string topic = "03-orders";

using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrap }).Build();

await RecreateTopic(admin, new TopicSpecification { Name = topic, NumPartitions = 3, ReplicationFactor = 3 });

using var producer = new ProducerBuilder<Null, string>(new ProducerConfig { BootstrapServers = bootstrap }).Build();

// write 7 events: partition = i % 3, so partitions get 3, 2 and 2 events
for (var i = 0; i < 7; i++)
{
    var partition = new TopicPartition(topic, new Partition(i % 3));

    var result = await producer.ProduceAsync(partition, new Message<Null, string> { Value = $"order-{i}" });

    Console.WriteLine($"order-{i} -> partition {result.Partition.Value}, offset {result.Offset.Value}");
}

using var consumer = new ConsumerBuilder<Null, string>(new ConsumerConfig { BootstrapServers = bootstrap, GroupId = "03-inspector" }).Build();

Console.WriteLine();

for (var p = 0; p < 3; p++)
{
    // low = first offset still stored, high = next offset to be written
    var marks = consumer.QueryWatermarkOffsets(new TopicPartition(topic, p), TimeSpan.FromSeconds(5));

    Console.WriteLine($"partition {p}: low={marks.Low.Value} high={marks.High.Value} -> {marks.High.Value - marks.Low.Value} events");
}

var description = await admin.DescribeTopicsAsync(TopicCollection.OfTopicNames([topic]));

Console.WriteLine();

foreach (var p in description.TopicDescriptions[0].Partitions)
{
    Console.WriteLine($"partition {p.Partition}: leader={p.Leader.Id} replicas=[{string.Join(",", p.Replicas.Select(r => r.Id))}] isr=[{string.Join(",", p.ISR.Select(r => r.Id))}]");
}

static async Task RecreateTopic(IAdminClient admin, TopicSpecification spec)
{
    try
    {
        await admin.DeleteTopicsAsync([spec.Name]);

        await Task.Delay(1000);
    }
    catch (DeleteTopicsException) { }

    await admin.CreateTopicsAsync([spec]);

    await Task.Delay(500);
}
