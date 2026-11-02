// 05 - Consumer: subscribe, poll loop, graceful close.
using Confluent.Kafka;
using Confluent.Kafka.Admin;

const string bootstrap = "localhost:9092,localhost:9094,localhost:9096";

const string topic = "05-shipments";

using (var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrap }).Build())
{
    try
    {
        await admin.DeleteTopicsAsync([topic]);

        await Task.Delay(1000);
    }
    catch (DeleteTopicsException) { }

    await admin.CreateTopicsAsync([new TopicSpecification { Name = topic, NumPartitions = 3, ReplicationFactor = 3 }]);
}

using (var producer = new ProducerBuilder<string, string>(new ProducerConfig { BootstrapServers = bootstrap }).Build())
{
    for (var i = 1; i <= 6; i++)
    {
        await producer.ProduceAsync(topic, new Message<string, string> { Key = $"ship-{i}", Value = $"parcel {i} left warehouse" });
    }
}

var config = new ConsumerConfig
{
    BootstrapServers = bootstrap,
    GroupId = "05-shipment-tracker",
    AutoOffsetReset = AutoOffsetReset.Earliest,
    EnableAutoCommit = true
};

using var consumer = new ConsumerBuilder<string, string>(config).Build();

consumer.Subscribe(topic);

using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));

Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };

var count = 0;

try
{
    while (count < 6)
    {
        // blocks until a record arrives or the token is cancelled
        var record = consumer.Consume(cts.Token);

        count++;

        Console.WriteLine($"p{record.Partition.Value}@{record.Offset.Value} key={record.Message.Key} value=\"{record.Message.Value}\"");
    }
}
catch (OperationCanceledException)
{
    Console.WriteLine("stopped by timeout / Ctrl+C");
}
finally
{
    // leaves the group cleanly and commits the final offsets
    consumer.Close();
}

Console.WriteLine($"consumed {count} records");
