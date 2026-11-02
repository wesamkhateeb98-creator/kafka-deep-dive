// 23 - Testing: a real Kafka broker per test class with Testcontainers.
using Confluent.Kafka;
using Testcontainers.Kafka;
using Xunit;

namespace KafkaTesting;

public sealed class OrderEventsTests : IAsyncLifetime
{
    private readonly KafkaContainer _kafka = new KafkaBuilder("apache/kafka:4.1.0").Build();

    public async Task InitializeAsync() => await _kafka.StartAsync();

    public async Task DisposeAsync() => await _kafka.DisposeAsync();

    [Fact]
    public async Task Produced_event_is_consumed_with_same_key_and_value()
    {
        var bootstrap = _kafka.GetBootstrapAddress();

        using var producer = new ProducerBuilder<string, string>(new ProducerConfig { BootstrapServers = bootstrap }).Build();

        await producer.ProduceAsync("orders", new Message<string, string> { Key = "ord-1", Value = "OrderPlaced" });

        var config = new ConsumerConfig { BootstrapServers = bootstrap, GroupId = "test", AutoOffsetReset = AutoOffsetReset.Earliest };

        using var consumer = new ConsumerBuilder<string, string>(config).Build();

        consumer.Subscribe("orders");

        var record = consumer.Consume(TimeSpan.FromSeconds(30));

        Assert.NotNull(record);

        Assert.Equal("ord-1", record.Message.Key);

        Assert.Equal("OrderPlaced", record.Message.Value);
    }

    [Fact]
    public async Task Same_key_always_goes_to_same_partition()
    {
        var bootstrap = _kafka.GetBootstrapAddress();

        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrap }).Build();

        await admin.CreateTopicsAsync([new Confluent.Kafka.Admin.TopicSpecification { Name = "accounts", NumPartitions = 4, ReplicationFactor = 1 }]);

        using var producer = new ProducerBuilder<string, string>(new ProducerConfig { BootstrapServers = bootstrap }).Build();

        var partitions = new HashSet<int>();

        for (var i = 0; i < 10; i++)
        {
            var result = await producer.ProduceAsync("accounts", new Message<string, string> { Key = "acc-42", Value = $"event-{i}" });

            partitions.Add(result.Partition.Value);
        }

        Assert.Single(partitions);
    }
}
