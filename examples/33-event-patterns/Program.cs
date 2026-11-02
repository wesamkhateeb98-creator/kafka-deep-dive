// 33 - Event-driven patterns: choreography saga with compensation, idempotent consumer, event sourcing replay.
using System.Text;
using Confluent.Kafka;
using Confluent.Kafka.Admin;

const string bootstrap = "localhost:9092,localhost:9094,localhost:9096";

const string orders = "33-orders";

const string payments = "33-payments";

using (var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrap }).Build())
{
    try
    {
        await admin.DeleteTopicsAsync([orders, payments]);

        await Task.Delay(1000);
    }
    catch (DeleteTopicsException) { }

    await admin.CreateTopicsAsync(
    [
        new TopicSpecification { Name = orders, NumPartitions = 1, ReplicationFactor = 3 },
        new TopicSpecification { Name = payments, NumPartitions = 1, ReplicationFactor = 3 }
    ]);

    await Task.Delay(500);
}

using var producer = new ProducerBuilder<string, string>(new ProducerConfig { BootstrapServers = bootstrap, EnableIdempotence = true }).Build();

// 1. Order service: publishes OrderPlaced (o-1 is delivered TWICE, as at-least-once delivery can do)
await Publish(orders, "o-1", "OrderPlaced:50", eventId: "evt-1");

await Publish(orders, "o-2", "OrderPlaced:500", eventId: "evt-2");

await Publish(orders, "o-1", "OrderPlaced:50", eventId: "evt-1");

await Publish(orders, "o-3", "OrderPlaced:30", eventId: "evt-3");

// 2. Payment service: idempotent consumer (dedupe by event id), wallet = 100 EUR
Console.WriteLine("== payment service (wallet 100 EUR)");

var processed = new HashSet<string>();

var wallet = 100;

foreach (var r in ReadAll(orders))
{
    var eventId = Header(r, "event-id");

    if (!processed.Add(eventId))
    {
        Console.WriteLine($"  {r.Message.Key}: duplicate {eventId} skipped (not charged twice)");

        continue;
    }

    var amount = int.Parse(r.Message.Value.Split(':')[1]);

    if (amount <= wallet)
    {
        wallet -= amount;

        await Publish(payments, r.Message.Key, $"PaymentSucceeded:{amount}", eventId: $"pay-{eventId}");

        Console.WriteLine($"  {r.Message.Key}: charged {amount}, wallet now {wallet}");
    }
    else
    {
        await Publish(payments, r.Message.Key, $"PaymentFailed:{amount}", eventId: $"pay-{eventId}");

        Console.WriteLine($"  {r.Message.Key}: {amount} > wallet {wallet} -> PaymentFailed");
    }
}

// 3. Order service reacts to payment events: confirm, or compensate (cancel)
Console.WriteLine("== order service (saga outcome)");

foreach (var r in ReadAll(payments))
{
    var outcome = r.Message.Value.StartsWith("PaymentSucceeded") ? "CONFIRMED" : "CANCELLED (compensation: release stock, notify customer)";

    Console.WriteLine($"  {r.Message.Key}: {outcome}");
}

// 4. Event sourcing: rebuild the wallet balance only from the payment events
var rebuilt = 100 - ReadAll(payments).Where(r => r.Message.Value.StartsWith("PaymentSucceeded")).Sum(r => int.Parse(r.Message.Value.Split(':')[1]));

Console.WriteLine($"== event sourcing: wallet rebuilt from {ReadAll(payments).Count} payment events = {rebuilt} (live value {wallet})");

async Task Publish(string topic, string key, string value, string eventId)
{
    var headers = new Headers { { "event-id", Encoding.UTF8.GetBytes(eventId) } };

    await producer.ProduceAsync(topic, new Message<string, string> { Key = key, Value = value, Headers = headers });
}

List<ConsumeResult<string, string>> ReadAll(string topic)
{
    using var consumer = new ConsumerBuilder<string, string>(new ConsumerConfig { BootstrapServers = bootstrap, GroupId = "33-reader" }).Build();

    consumer.Assign(new TopicPartitionOffset(topic, 0, Offset.Beginning));

    var records = new List<ConsumeResult<string, string>>();

    while (consumer.Consume(TimeSpan.FromSeconds(2)) is { } r)
    {
        records.Add(r);
    }

    return records;
}

static string Header(ConsumeResult<string, string> r, string name) => Encoding.UTF8.GetString(r.Message.Headers.GetLastBytes(name));
