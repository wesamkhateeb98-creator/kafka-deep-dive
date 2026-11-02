// 17 - Stream processing with Streamiz (Kafka Streams for .NET): count big orders per customer.
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Table;

const string bootstrap = "localhost:9092,localhost:9094,localhost:9096";

const string input = "17-orders";

const string output = "17-big-order-counts";

using (var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrap }).Build())
{
    try
    {
        await admin.DeleteTopicsAsync([input, output]);

        await Task.Delay(1000);
    }
    catch (DeleteTopicsException) { }

    await admin.CreateTopicsAsync(
    [
        new TopicSpecification { Name = input, NumPartitions = 1, ReplicationFactor = 3 },
        new TopicSpecification { Name = output, NumPartitions = 1, ReplicationFactor = 3 }
    ]);

    await Task.Delay(500);
}

var config = new StreamConfig<StringSerDes, StringSerDes>
{
    ApplicationId = $"17-order-counter-{DateTime.UtcNow:HHmmss}",
    BootstrapServers = bootstrap,
    AutoOffsetReset = AutoOffsetReset.Earliest,
    CommitIntervalMs = 500,
    ReplicationFactor = 3
};

// topology: orders -> filter amount >= 100 -> group by customer -> count -> output topic
var builder = new StreamBuilder();

builder.Stream<string, string>(input)
    .Filter((customer, amount, _) => decimal.Parse(amount) >= 100)
    .GroupByKey()
    .Count(InMemory.As<string, long>("big-order-count-store"))
    .ToStream()
    .MapValues((count, _) => count.ToString())
    .To(output);

var topology = builder.Build();

Console.WriteLine(topology.Describe());

var stream = new KafkaStream(topology, config);

await stream.StartAsync();

using (var producer = new ProducerBuilder<string, string>(new ProducerConfig { BootstrapServers = bootstrap }).Build())
{
    (string Customer, string Amount)[] orders = [("alice", "150"), ("bob", "30"), ("alice", "20"), ("bob", "300"), ("alice", "500"), ("carol", "99")];

    foreach (var (customer, amount) in orders)
    {
        await producer.ProduceAsync(input, new Message<string, string> { Key = customer, Value = amount });

        Console.WriteLine($"order in : {customer} {amount}");
    }
}

await Task.Delay(TimeSpan.FromSeconds(8));

stream.Dispose();

using var consumer = new ConsumerBuilder<string, string>(new ConsumerConfig { BootstrapServers = bootstrap, GroupId = "17-reader" }).Build();

consumer.Assign(new TopicPartitionOffset(output, 0, Offset.Beginning));

while (consumer.Consume(TimeSpan.FromSeconds(3)) is { } record)
{
    Console.WriteLine($"count out: {record.Message.Key} -> {record.Message.Value} big order(s)");
}
