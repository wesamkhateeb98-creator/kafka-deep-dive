// 02 — Producer: key, value, headers, timestamp → where did it land (partition/offset)?
// Run: dotnet run --project 02-Producer
using System.Text;
using System.Text.Json;
using Confluent.Kafka;

const string topic = "demo.orders";
await Cluster.EnsureTopicAsync(topic, partitions: 6);

var config = new ProducerConfig
{
    BootstrapServers = Cluster.Bootstrap,
    ClientId = "demo-producer",
    Acks = Acks.All,                        // wait for all in-sync replicas
    EnableIdempotence = true,               // retries never create duplicates
    LingerMs = 5,                           // wait up to 5ms to build a batch
    CompressionType = CompressionType.Lz4,
    Partitioner = Partitioner.Murmur2Random // same key→partition mapping as Java clients
};

using var producer = new ProducerBuilder<string, string>(config).Build();

// A) await each send: simple, 1 round-trip per message (slow for bulk)
for (var i = 1; i <= 6; i++)
{
    var customerId = $"customer-{i % 3}";
    var order = new { OrderId = 1000 + i, CustomerId = customerId, Total = i * 10.5m };

    var result = await producer.ProduceAsync(topic, new Message<string, string>
    {
        Key = customerId,                                       // decides the partition
        Value = JsonSerializer.Serialize(order),                // the payload (bytes on the wire)
        Headers = new Headers { { "event-type", Encoding.UTF8.GetBytes("order.created.v1") } },
        Timestamp = new Timestamp(DateTime.UtcNow)              // CreateTime (default anyway)
    });

    Console.WriteLine($"key={customerId,-11} → partition {result.Partition.Value}, offset {result.Offset.Value,-4} [{result.Status}]");
}

// B) fire-and-forget + delivery callback: high throughput, batches fill up
var delivered = 0;
for (var i = 0; i < 1_000; i++)
{
    producer.Produce(topic, new Message<string, string> { Key = $"customer-{i % 100}", Value = $"bulk-{i}" },
        report =>
        {
            if (report.Error.IsError) Console.WriteLine($"❌ {report.Error.Reason}");
            else Interlocked.Increment(ref delivered);
        });
}

producer.Flush(TimeSpan.FromSeconds(30));   // block until every in-flight batch is acknowledged
Console.WriteLine($"\nbulk delivered: {delivered}/1000");
