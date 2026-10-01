// 08 — Retry with backoff + Dead Letter Queue (a poison message must not block the partition)
// Run: dotnet run --project 08-RetryDlq
using System.Text;
using Confluent.Kafka;

const string topic = "demo.shipments";
const string dlq = "demo.shipments.dlq";
const int maxAttempts = 3;
await Cluster.EnsureTopicAsync(topic, partitions: 1);
await Cluster.EnsureTopicAsync(dlq, partitions: 1);

using var producer = new ProducerBuilder<string, string>(new ProducerConfig
{
    BootstrapServers = Cluster.Bootstrap,
    EnableIdempotence = true
}).Build();

// Seed: ship-3 is a poison pill, ship-4 fails twice then succeeds (transient)
var runId = Guid.NewGuid().ToString("N")[..6];
foreach (var id in new[] { "ship-1", "ship-2", "POISON-3", "FLAKY-4", "ship-5" })
    await producer.ProduceAsync(topic, new Message<string, string> { Key = $"{runId}-{id}", Value = id });

using var consumer = new ConsumerBuilder<string, string>(new ConsumerConfig
{
    BootstrapServers = Cluster.Bootstrap,
    GroupId = $"demo-dlq-{runId}",
    AutoOffsetReset = AutoOffsetReset.Latest,   // new group; we assign explicitly below
    EnableAutoCommit = false
}).Build();

// read only this run's 5 records: seek to (end - 5)
var tp = new TopicPartition(topic, 0);
var end = consumer.QueryWatermarkOffsets(tp, TimeSpan.FromSeconds(5)).High;
consumer.Assign(new TopicPartitionOffset(tp, end - 5));

var flakyAttempts = 0;
void Process(string value)
{
    if (value.StartsWith("POISON")) throw new InvalidOperationException("address does not exist");
    if (value.StartsWith("FLAKY") && ++flakyAttempts <= 2) throw new TimeoutException("carrier API timeout");
    Console.WriteLine($"   ✅ shipped {value}");
}

for (var i = 0; i < 5; i++)
{
    var r = consumer.Consume(TimeSpan.FromSeconds(10))!;
    Console.WriteLine($"p{r.Partition.Value}@{r.Offset.Value} {r.Message.Value}");

    for (var attempt = 1; ; attempt++)
    {
        try { Process(r.Message.Value); break; }
        catch (Exception ex) when (attempt < maxAttempts)
        {
            var backoff = 200 * (int)Math.Pow(2, attempt - 1);          // 200, 400, 800 ms
            Console.WriteLine($"   🔁 attempt {attempt} failed: {ex.Message} → retry in {backoff}ms");
            await Task.Delay(backoff);
        }
        catch (Exception ex)
        {
            await producer.ProduceAsync(dlq, new Message<string, string>
            {
                Key = r.Message.Key,
                Value = r.Message.Value,
                Headers = new Headers
                {
                    { "dlq-reason", Encoding.UTF8.GetBytes(ex.Message) },
                    { "dlq-original", Encoding.UTF8.GetBytes($"{r.Topic}[{r.Partition.Value}]@{r.Offset.Value}") },
                    { "dlq-attempts", Encoding.UTF8.GetBytes(attempt.ToString()) }
                }
            });
            Console.WriteLine($"   ☠️ → {dlq}: {ex.Message}");
            break;
        }
    }

    consumer.Commit(r);   // move on either way: success OR parked in DLQ
}

Console.WriteLine("\npartition never got stuck: ship-5 was processed after the poison pill ✔");
