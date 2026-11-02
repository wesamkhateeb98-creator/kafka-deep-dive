// 04 - Producer: async send with headers, fire-and-callback send, flush.
using System.Text;
using Confluent.Kafka;

const string bootstrap = "localhost:9092,localhost:9094,localhost:9096";

const string topic = "04-payments";

var config = new ProducerConfig
{
    BootstrapServers = bootstrap,
    ClientId = "payments-api",
    Acks = Acks.All,
    EnableIdempotence = true,
    LingerMs = 5
};

using var producer = new ProducerBuilder<string, string>(config).Build();

// style 1: await each send (simple, slower: one round trip per message)
var message = new Message<string, string>
{
    Key = "pay-1001",
    Value = """{"amount":250,"currency":"EUR"}""",
    Headers = new Headers { { "event-type", Encoding.UTF8.GetBytes("PaymentCreated") } }
};

var result = await producer.ProduceAsync(topic, message);

Console.WriteLine($"[await]    key={result.Message.Key} -> p{result.Partition.Value}@{result.Offset.Value} status={result.Status} ts={result.Timestamp.UtcDateTime:O}");

// style 2: Produce + delivery callback (fast: many messages in flight, batched)
var delivered = 0;

for (var i = 1; i <= 5; i++)
{
    producer.Produce(topic, new Message<string, string> { Key = $"pay-200{i}", Value = $$"""{"amount":{{i * 10}}}""" }, report =>
    {
        Interlocked.Increment(ref delivered);

        Console.WriteLine($"[callback] key={report.Message.Key} -> p{report.Partition.Value}@{report.Offset.Value} error={report.Error.Code}");
    });
}

// Produce() only queues; Flush waits until every queued message is acknowledged
var stillQueued = producer.Flush(TimeSpan.FromSeconds(10));

Console.WriteLine($"delivered={delivered} stillQueued={stillQueued}");
