// 11 - Errors, retries and a dead letter queue (DLQ).
using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using Confluent.Kafka.Admin;

const string bootstrap = "localhost:9092,localhost:9094,localhost:9096";

const string topic = "11-orders";

const string dlqTopic = "11-orders.dlq";

const int maxAttempts = 3;

using (var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrap }).Build())
{
    try
    {
        await admin.DeleteTopicsAsync([topic, dlqTopic]);

        await Task.Delay(1000);
    }
    catch (DeleteTopicsException) { }

    await admin.CreateTopicsAsync(
    [
        new TopicSpecification { Name = topic, NumPartitions = 1, ReplicationFactor = 3 },
        new TopicSpecification { Name = dlqTopic, NumPartitions = 1, ReplicationFactor = 3 }
    ]);

    await Task.Delay(500);
}

using var producer = new ProducerBuilder<string, string>(new ProducerConfig { BootstrapServers = bootstrap }).Build();

string[] payloads =
[
    """{"id":1,"amount":40}""",
    "not-json",
    """{"id":3,"amount":-5}""",
    """{"id":4,"amount":70,"flaky":2}""",
    """{"id":5,"amount":90,"flaky":9}"""
];

foreach (var payload in payloads)
{
    await producer.ProduceAsync(topic, new Message<string, string> { Key = "store-7", Value = payload });
}

var flakyCalls = new Dictionary<long, int>();

var config = new ConsumerConfig
{
    BootstrapServers = bootstrap,
    GroupId = "11-order-processor",
    AutoOffsetReset = AutoOffsetReset.Earliest,
    EnableAutoCommit = false
};

using var consumer = new ConsumerBuilder<string, string>(config).Build();

consumer.Subscribe(topic);

while (consumer.Consume(TimeSpan.FromSeconds(5)) is { } record)
{
    var attempt = 0;

    while (true)
    {
        attempt++;

        try
        {
            Process(record);

            Console.WriteLine($"offset {record.Offset.Value}: OK (attempt {attempt})");

            break;
        }
        catch (TransientException ex) when (attempt < maxAttempts)
        {
            // retryable: wait with exponential backoff, then try again
            var delay = TimeSpan.FromMilliseconds(100 * Math.Pow(2, attempt - 1));

            Console.WriteLine($"offset {record.Offset.Value}: transient error \"{ex.Message}\" -> retry in {delay.TotalMilliseconds} ms");

            await Task.Delay(delay);
        }
        catch (Exception ex)
        {
            // poison / non-retryable / retries exhausted: park it in the DLQ and move on
            await SendToDlq(record, ex, attempt);

            Console.WriteLine($"offset {record.Offset.Value}: -> DLQ after {attempt} attempt(s): {ex.GetType().Name}: {ex.Message}");

            break;
        }
    }

    // commit only after the record is either processed or safely in the DLQ
    consumer.Commit(record);
}

consumer.Close();

Console.WriteLine();

using var dlqReader = new ConsumerBuilder<string, string>(new ConsumerConfig { BootstrapServers = bootstrap, GroupId = "11-dlq-inspector" }).Build();

dlqReader.Assign(new TopicPartitionOffset(dlqTopic, 0, Offset.Beginning));

while (dlqReader.Consume(TimeSpan.FromSeconds(3)) is { } dead)
{
    var headers = string.Join(" ", dead.Message.Headers.Select(h => $"{h.Key}={Encoding.UTF8.GetString(h.GetValueBytes())}"));

    Console.WriteLine($"DLQ value={dead.Message.Value} | {headers}");
}

void Process(ConsumeResult<string, string> record)
{
    var order = JsonDocument.Parse(record.Message.Value).RootElement;

    if (order.GetProperty("amount").GetInt32() <= 0)
    {
        throw new ArgumentException("amount must be positive");
    }

    // "flaky": N means the downstream call fails N times before it works
    if (order.TryGetProperty("flaky", out var flaky))
    {
        var calls = flakyCalls[record.Offset.Value] = flakyCalls.GetValueOrDefault(record.Offset.Value) + 1;

        if (calls <= flaky.GetInt32())
        {
            throw new TransientException($"payment service timeout #{calls}");
        }
    }
}

async Task SendToDlq(ConsumeResult<string, string> record, Exception ex, int attempts)
{
    var headers = new Headers
    {
        { "error", Encoding.UTF8.GetBytes(ex.GetType().Name) },
        { "source", Encoding.UTF8.GetBytes($"{record.Topic}/{record.Partition.Value}/{record.Offset.Value}") },
        { "attempts", Encoding.UTF8.GetBytes(attempts.ToString()) }
    };

    await producer.ProduceAsync(dlqTopic, new Message<string, string> { Key = record.Message.Key, Value = record.Message.Value, Headers = headers });
}

class TransientException(string message) : Exception(message);
