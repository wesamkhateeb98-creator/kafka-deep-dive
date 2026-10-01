// 06 — Producer tuning: acks × linger × compression → throughput (numbers, not opinions)
// Run: dotnet run -c Release --project 06-ProducerTuning
using System.Diagnostics;
using Confluent.Kafka;

const string topic = "demo.bench";
const int messages = 50_000;
await Cluster.EnsureTopicAsync(topic, partitions: 6);

var payload = new string('x', 512);   // 512-byte value (JSON-ish size)

(string Name, ProducerConfig Config)[] scenarios =
[
    ("acks=0  linger=0  none", Build(Acks.None, 0, CompressionType.None)),
    ("acks=1  linger=0  none", Build(Acks.Leader, 0, CompressionType.None)),
    ("acks=all linger=0  none", Build(Acks.All, 0, CompressionType.None)),
    ("acks=all linger=20 none", Build(Acks.All, 20, CompressionType.None)),
    ("acks=all linger=20 lz4 ", Build(Acks.All, 20, CompressionType.Lz4)),
    ("acks=all linger=20 zstd", Build(Acks.All, 20, CompressionType.Zstd)),
];

Console.WriteLine($"{messages:N0} msgs × {payload.Length} B\n");
Console.WriteLine("scenario                  | msgs/s    | MB/s");
foreach (var (name, config) in scenarios)
{
    using var producer = new ProducerBuilder<Null, string>(config).Build();
    await producer.ProduceAsync(topic, new Message<Null, string> { Value = "warm-up" });  // connections + metadata + PID
    var sw = Stopwatch.StartNew();

    for (var i = 0; i < messages; i++)
    {
        while (true)
        {
            try { producer.Produce(topic, new Message<Null, string> { Value = payload }); break; }
            catch (ProduceException<Null, string> e) when (e.Error.Code == ErrorCode.Local_QueueFull)
            {
                producer.Poll(TimeSpan.FromMilliseconds(50));   // local buffer full → let batches drain
            }
        }
    }
    producer.Flush(TimeSpan.FromMinutes(1));
    sw.Stop();

    var rate = messages / sw.Elapsed.TotalSeconds;
    Console.WriteLine($"{name,-25} | {rate,9:N0} | {rate * payload.Length / 1_048_576,5:N1}");
}

static ProducerConfig Build(Acks acks, int lingerMs, CompressionType compression) => new()
{
    BootstrapServers = Cluster.Bootstrap,
    Acks = acks,
    EnableIdempotence = acks == Acks.All,   // idempotence requires acks=all
    LingerMs = lingerMs,
    CompressionType = compression,
    BatchSize = 256 * 1024
};
