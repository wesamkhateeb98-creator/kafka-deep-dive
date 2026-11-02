// 32 - MirrorMaker 2: replicate topics and consumer offsets to a disaster-recovery cluster.
// Needs: docker compose --profile dr up -d
using Confluent.Kafka;
using Confluent.Kafka.Admin;

const string primary = "localhost:9092,localhost:9094,localhost:9096";

const string dr = "localhost:9292";

// fresh names per run (MirrorMaker copies every topic and group matching 32-.*)
var run = DateTime.UtcNow.ToString("HHmmss");

var topic = $"32-orders-{run}";

var group = $"32-billing-{run}";

using (var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = primary }).Build())
{
    try
    {
        await admin.CreateTopicsAsync([new TopicSpecification { Name = topic, NumPartitions = 1, ReplicationFactor = 3 }]);
    }
    catch (CreateTopicsException) { }
}

using (var producer = new ProducerBuilder<string, string>(new ProducerConfig { BootstrapServers = primary }).Build())
{
    for (var i = 1; i <= 5; i++)
    {
        await producer.ProduceAsync(topic, new Message<string, string> { Key = $"o-{i}", Value = $"order {i}" });
    }
}

// billing processes 3 of 5 on the primary cluster, commits, stops
var primaryConfig = new ConsumerConfig { BootstrapServers = primary, GroupId = group, AutoOffsetReset = AutoOffsetReset.Earliest, EnableAutoCommit = false };

using (var consumer = new ConsumerBuilder<string, string>(primaryConfig).Build())
{
    consumer.Subscribe(topic);

    for (var done = 0; done < 3;)
    {
        if (consumer.Consume(TimeSpan.FromSeconds(5)) is { } r)
        {
            done++;

            consumer.Commit(r);

            Console.WriteLine($"primary: billing processed {r.Message.Key} (offset {r.Offset.Value})");
        }
    }

    consumer.Close();
}

using var drAdmin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = dr }).Build();

var mirrored = $"primary.{topic}";

var translated = -1L;

// MM2 emits checkpoints every 5 s; translation can lag behind the real commit, so watch it change
for (var i = 0; i < 24 && translated < 3; i++)
{
    await Task.Delay(TimeSpan.FromSeconds(5));

    try
    {
        var offsets = await drAdmin.ListConsumerGroupOffsetsAsync([new ConsumerGroupTopicPartitions(group, [new TopicPartition(mirrored, 0)])]);

        var now = offsets[0].Partitions[0].Offset.Value;

        if (now != translated)
        {
            Console.WriteLine($"dr: after {(i + 1) * 5,3} s translated offset = {now}");

            translated = now;
        }
    }
    catch (KafkaException) { }
}

var drTopics = drAdmin.GetMetadata(TimeSpan.FromSeconds(10)).Topics.Select(t => t.Topic).Where(t => !t.StartsWith("__")).Order();

Console.WriteLine($"dr topics: {string.Join(", ", drTopics)}");

Console.WriteLine($"dr: committed offset of {group} on {mirrored} = {translated}");

// failover: the same group starts on the DR cluster and continues where it stopped
var drConfig = new ConsumerConfig { BootstrapServers = dr, GroupId = group, AutoOffsetReset = AutoOffsetReset.Earliest };

using var drConsumer = new ConsumerBuilder<string, string>(drConfig).Build();

drConsumer.Subscribe(mirrored);

while (drConsumer.Consume(TimeSpan.FromSeconds(8)) is { } r)
{
    Console.WriteLine($"dr: billing processed {r.Message.Key} (offset {r.Offset.Value})");
}

drConsumer.Close();
