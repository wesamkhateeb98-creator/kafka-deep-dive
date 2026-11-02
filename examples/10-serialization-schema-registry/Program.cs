// 10 - Serialization & Schema Registry (needs: docker compose --profile registry up -d).
using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;

const string bootstrap = "localhost:9092,localhost:9094,localhost:9096";

const string topic = "10-orders";

const string subject = $"{topic}-value";

using var registry = new CachedSchemaRegistryClient(new SchemaRegistryConfig { Url = "http://localhost:8081" });

// start clean so the output is the same on every run (soft delete, then hard delete)
using (var http = new HttpClient { BaseAddress = new Uri("http://localhost:8081") })
{
    await http.DeleteAsync($"/subjects/{subject}");

    await http.DeleteAsync($"/subjects/{subject}?permanent=true");
}

using (var producer = new ProducerBuilder<string, Order>(new ProducerConfig { BootstrapServers = bootstrap })
    .SetValueSerializer(new JsonSerializer<Order>(registry))
    .Build())
{
    // the serializer generates a JSON schema from Order and registers it on first use
    var result = await producer.ProduceAsync(topic, new Message<string, Order> { Key = "o-1", Value = new Order("o-1", "alice", 120.5m) });

    Console.WriteLine($"produced Order o-1 -> p{result.Partition.Value}@{result.Offset.Value}");

    var raw = await ReadRaw(result.TopicPartitionOffset);

    Console.WriteLine($"wire bytes: magic=0x{raw[0]:x2} schemaId=0x{Convert.ToHexString(raw, 1, 4)} json={System.Text.Encoding.UTF8.GetString(raw, 5, raw.Length - 5)}");
}

var latest = await registry.GetLatestSchemaAsync(subject);

Console.WriteLine($"subject={subject} version={latest.Version} id={latest.Id} type={latest.SchemaType}");

Console.WriteLine($"compatibility={await registry.GetCompatibilityAsync()}");

using (var consumer = new ConsumerBuilder<string, Order>(new ConsumerConfig { BootstrapServers = bootstrap, GroupId = "10-reader" })
    .SetValueDeserializer(new JsonDeserializer<Order>().AsSyncOverAsync())
    .Build())
{
    var marks = consumer.QueryWatermarkOffsets(new TopicPartition(topic, 0), TimeSpan.FromSeconds(5));

    consumer.Assign(new TopicPartitionOffset(topic, 0, marks.High - 1));

    var record = consumer.Consume(TimeSpan.FromSeconds(10));

    Console.WriteLine($"consumed typed object: {record.Message.Value}");
}

// a breaking change: Amount becomes a string -> the registry rejects it (BACKWARD compatibility)
var breaking = latest.SchemaString.Replace("\"number\"", "\"string\"");

try
{
    await registry.RegisterSchemaAsync(subject, new Schema(breaking, SchemaType.Json));

    Console.WriteLine("breaking schema accepted (unexpected)");
}
catch (SchemaRegistryException ex)
{
    Console.WriteLine($"breaking schema rejected: HTTP {(int)ex.Status} {ex.Message.Split(';')[0]}");
}

async Task<byte[]> ReadRaw(TopicPartitionOffset at)
{
    using var raw = new ConsumerBuilder<Ignore, byte[]>(new ConsumerConfig { BootstrapServers = bootstrap, GroupId = "10-raw" }).Build();

    raw.Assign(at);

    await Task.Yield();

    return raw.Consume(TimeSpan.FromSeconds(10)).Message.Value;
}

public record Order(string Id, string Customer, decimal Amount);
