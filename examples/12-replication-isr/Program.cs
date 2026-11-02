// 12 - Replication & ISR: watch leaders and in-sync replicas while brokers die.
// usage: dotnet run create   -> (re)create the topic: 3 partitions, RF=3, min.insync.replicas=2
//        dotnet run          -> describe partitions + try one acks=all write
using Confluent.Kafka;
using Confluent.Kafka.Admin;

const string bootstrap = "localhost:9092,localhost:9094,localhost:9096";

const string topic = "12-bank-transfers";

using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrap })
    .SetLogHandler((_, _) => { })
    .Build();

if (args.FirstOrDefault() == "create")
{
    try
    {
        await admin.DeleteTopicsAsync([topic]);

        await Task.Delay(1000);
    }
    catch (DeleteTopicsException) { }

    var spec = new TopicSpecification
    {
        Name = topic,
        NumPartitions = 3,
        ReplicationFactor = 3,
        Configs = new() { ["min.insync.replicas"] = "2" }
    };

    await admin.CreateTopicsAsync([spec]);

    await Task.Delay(1000);
}

var cluster = await admin.DescribeClusterAsync();

Console.WriteLine($"alive brokers: [{string.Join(",", cluster.Nodes.Select(n => n.Id).Order())}]");

var description = await admin.DescribeTopicsAsync(TopicCollection.OfTopicNames([topic]));

foreach (var p in description.TopicDescriptions[0].Partitions)
{
    var leader = p.Leader?.Id.ToString() ?? "none";

    Console.WriteLine($"partition {p.Partition}: leader={leader} replicas=[{string.Join(",", p.Replicas.Select(r => r.Id))}] isr=[{string.Join(",", p.ISR.Select(r => r.Id))}]");
}

var config = new ProducerConfig
{
    BootstrapServers = bootstrap,
    Acks = Acks.All,
    MessageTimeoutMs = 5000,
    // no client retries, so the broker's error is shown instead of a timeout
    MessageSendMaxRetries = 0
};

using var producer = new ProducerBuilder<string, string>(config)
    .SetLogHandler((_, _) => { })
    .Build();

try
{
    var result = await producer.ProduceAsync(new TopicPartition(topic, 0), new Message<string, string> { Key = "tx-1", Value = "100 EUR" });

    Console.WriteLine($"acks=all write -> OK partition {result.Partition.Value} offset {result.Offset.Value}");
}
catch (ProduceException<string, string> ex)
{
    Console.WriteLine($"acks=all write -> FAILED: {ex.Error.Code} ({ex.Error.Reason})");
}
