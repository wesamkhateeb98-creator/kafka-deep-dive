// 16 - Transactions & exactly-once: committed vs aborted writes, and an atomic consume-transform-produce step.
using Confluent.Kafka;
using Confluent.Kafka.Admin;

const string bootstrap = "localhost:9092,localhost:9094,localhost:9096";

const string input = "16-transfers";

const string output = "16-ledger";

using (var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrap }).Build())
{
    try
    {
        await admin.DeleteTopicsAsync([input, output]);

        await Task.Delay(1000);

        await admin.DeleteGroupsAsync(["16-ledger-writer"]);
    }
    catch (KafkaException) { }

    await admin.CreateTopicsAsync(
    [
        new TopicSpecification { Name = input, NumPartitions = 1, ReplicationFactor = 3 },
        new TopicSpecification { Name = output, NumPartitions = 1, ReplicationFactor = 3 }
    ]);

    await Task.Delay(500);
}

// --- part 1: one committed and one aborted transaction ---
var producerConfig = new ProducerConfig { BootstrapServers = bootstrap, TransactionalId = "16-transfer-api" };

using (var producer = new ProducerBuilder<string, string>(producerConfig).Build())
{
    producer.InitTransactions(TimeSpan.FromSeconds(10));

    producer.BeginTransaction();

    producer.Produce(input, new Message<string, string> { Key = "alice", Value = "send 50 to bob" });

    producer.Produce(input, new Message<string, string> { Key = "bob", Value = "send 20 to carol" });

    producer.CommitTransaction();

    producer.BeginTransaction();

    producer.Produce(input, new Message<string, string> { Key = "carol", Value = "send 999 to mallory" });

    producer.Produce(input, new Message<string, string> { Key = "dave", Value = "send 10 to erin" });

    // make sure both records really reach the broker before we abort
    producer.Flush(TimeSpan.FromSeconds(10));

    producer.AbortTransaction();
}

Console.WriteLine($"read_uncommitted sees: {string.Join(" | ", Read(input, IsolationLevel.ReadUncommitted))}");

Console.WriteLine($"read_committed   sees: {string.Join(" | ", Read(input, IsolationLevel.ReadCommitted))}");

// --- part 2: consume-transform-produce; output write + input offset commit are ONE atomic unit ---
var consumerConfig = new ConsumerConfig
{
    BootstrapServers = bootstrap,
    GroupId = "16-ledger-writer",
    AutoOffsetReset = AutoOffsetReset.Earliest,
    EnableAutoCommit = false,
    IsolationLevel = IsolationLevel.ReadCommitted
};

using var consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();

using var txProducer = new ProducerBuilder<string, string>(new ProducerConfig { BootstrapServers = bootstrap, TransactionalId = "16-ledger-writer-0" }).Build();

txProducer.InitTransactions(TimeSpan.FromSeconds(10));

consumer.Subscribe(input);

while (consumer.Consume(TimeSpan.FromSeconds(5)) is { } record)
{
    txProducer.BeginTransaction();

    txProducer.Produce(output, new Message<string, string> { Key = record.Message.Key, Value = $"DEBIT {record.Message.Key}: {record.Message.Value}" });

    // the consumer offset is committed inside the producer's transaction
    txProducer.SendOffsetsToTransaction([new TopicPartitionOffset(record.TopicPartition, record.Offset + 1)], consumer.ConsumerGroupMetadata, TimeSpan.FromSeconds(10));

    txProducer.CommitTransaction();

    Console.WriteLine($"tx committed: in offset {record.Offset.Value} -> ledger + offset {record.Offset.Value + 1}");
}

consumer.Close();

Console.WriteLine($"ledger (read_committed): {string.Join(" | ", Read(output, IsolationLevel.ReadCommitted))}");

List<string> Read(string topic, IsolationLevel isolation)
{
    var config = new ConsumerConfig { BootstrapServers = bootstrap, GroupId = "16-inspector", IsolationLevel = isolation };

    using var reader = new ConsumerBuilder<string, string>(config).Build();

    reader.Assign(new TopicPartitionOffset(topic, 0, Offset.Beginning));

    var values = new List<string>();

    while (reader.Consume(TimeSpan.FromSeconds(2)) is { } r)
    {
        values.Add($"{r.Message.Key}@{r.Offset.Value}");
    }

    return values;
}
