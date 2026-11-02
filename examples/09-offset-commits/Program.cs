// 09 - Offset commits: commit AFTER processing = at-least-once (duplicates), BEFORE = at-most-once (loss).
using Confluent.Kafka;
using Confluent.Kafka.Admin;

const string bootstrap = "localhost:9092,localhost:9094,localhost:9096";

const string topic = "09-invoices";

using (var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrap }).Build())
{
    try
    {
        await admin.DeleteTopicsAsync([topic]);

        await Task.Delay(1000);

        await admin.DeleteGroupsAsync(["09-at-least-once", "09-at-most-once"]);
    }
    catch (KafkaException) { }

    await admin.CreateTopicsAsync([new TopicSpecification { Name = topic, NumPartitions = 1, ReplicationFactor = 3 }]);

    await Task.Delay(500);
}

using (var producer = new ProducerBuilder<Null, string>(new ProducerConfig { BootstrapServers = bootstrap }).Build())
{
    for (var i = 0; i < 6; i++)
    {
        await producer.ProduceAsync(topic, new Message<Null, string> { Value = $"invoice-{i}" });
    }
}

Console.WriteLine("== at-least-once: process, then commit ==");

Run("09-at-least-once", commitBeforeProcessing: false, crashAtOffset: 2);

Run("09-at-least-once", commitBeforeProcessing: false, crashAtOffset: null);

Console.WriteLine();

Console.WriteLine("== at-most-once: commit, then process ==");

Run("09-at-most-once", commitBeforeProcessing: true, crashAtOffset: 2);

Run("09-at-most-once", commitBeforeProcessing: true, crashAtOffset: null);

void Run(string group, bool commitBeforeProcessing, long? crashAtOffset)
{
    var config = new ConsumerConfig
    {
        BootstrapServers = bootstrap,
        GroupId = group,
        AutoOffsetReset = AutoOffsetReset.Earliest,
        EnableAutoCommit = false
    };

    using var consumer = new ConsumerBuilder<Null, string>(config).Build();

    consumer.Subscribe(topic);

    Console.WriteLine($"-- consumer started ({(crashAtOffset is null ? "runs to end" : $"will crash at offset {crashAtOffset}")})");

    while (consumer.Consume(TimeSpan.FromSeconds(5)) is { } record)
    {
        var offset = record.Offset.Value;

        if (commitBeforeProcessing)
        {
            consumer.Commit(record);
        }

        if (offset == crashAtOffset)
        {
            Console.WriteLine($"   offset {offset}: CRASH while processing {record.Message.Value}");

            break;
        }

        Console.WriteLine($"   offset {offset}: processed {record.Message.Value}");

        // at-least-once commits every 2 records to show the duplicate window
        if (!commitBeforeProcessing && offset % 2 == 1)
        {
            consumer.Commit(record);

            Console.WriteLine($"   commit -> next offset {offset + 1}");
        }
    }

    consumer.Close();
}
