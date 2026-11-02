// 20 - Performance tuning: same 200k records, different batching / compression settings.
using System.Diagnostics;
using System.Text;
using Confluent.Kafka;
using Confluent.Kafka.Admin;

const string bootstrap = "localhost:9092,localhost:9094,localhost:9096";

const int count = 200_000;

// realistic, compressible JSON (~112 bytes)
var payloads = Enumerable.Range(0, 1000)
    .Select(i => $$"""{"orderId":{{100000 + i}},"customer":"customer-{{i % 50}}","status":"CREATED","items":[{"sku":"SKU-{{i % 20}}","qty":{{i % 5 + 1}}}],"currency":"EUR"}""")
    .ToArray();

Console.WriteLine($"payload size ~{Encoding.UTF8.GetByteCount(payloads[0])} bytes, {count:N0} records, acks=all");

Console.WriteLine();

Console.WriteLine("setup                         |   msgs/s |  MB/s | elapsed");

Console.WriteLine("------------------------------+----------+-------+--------");

await Run("linger=0   no compression", lingerMs: 0, CompressionType.None);

await Run("linger=5   no compression", lingerMs: 5, CompressionType.None);

await Run("linger=20  no compression", lingerMs: 20, CompressionType.None);

await Run("linger=20  lz4", lingerMs: 20, CompressionType.Lz4);

await Run("linger=20  zstd", lingerMs: 20, CompressionType.Zstd);

async Task Run(string name, int lingerMs, CompressionType compression)
{
    var topic = $"20-perf-{compression}-{lingerMs}".ToLowerInvariant();

    using (var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrap }).Build())
    {
        try
        {
            await admin.DeleteTopicsAsync([topic]);

            await Task.Delay(1000);
        }
        catch (DeleteTopicsException) { }

        await admin.CreateTopicsAsync([new TopicSpecification { Name = topic, NumPartitions = 6, ReplicationFactor = 3 }]);

        await Task.Delay(500);
    }

    var config = new ProducerConfig
    {
        BootstrapServers = bootstrap,
        Acks = Acks.All,
        LingerMs = lingerMs,
        BatchSize = 256 * 1024,
        CompressionType = compression,
        QueueBufferingMaxMessages = 500_000
    };

    using var producer = new ProducerBuilder<string, string>(config).Build();

    long bytes = 0;

    var clock = Stopwatch.StartNew();

    for (var i = 0; i < count; i++)
    {
        var value = payloads[i % payloads.Length];

        bytes += value.Length;

        producer.Produce(topic, new Message<string, string> { Key = $"order-{i % 1000}", Value = value });
    }

    producer.Flush(TimeSpan.FromMinutes(2));

    var seconds = clock.Elapsed.TotalSeconds;

    Console.WriteLine($"{name,-29} | {count / seconds,8:N0} | {bytes / seconds / 1024 / 1024,5:0.0} | {seconds,5:0.00}s");
}
