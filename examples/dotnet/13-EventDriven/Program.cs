// 13 — Event-Driven Microservices: 1 event, 2 independent consumer groups (fan-out)
// 3 terminals:
//   dotnet run --project 13-EventDriven -- api            → http://localhost:5080  (POST /orders)
//   dotnet run --project 13-EventDriven -- inventory      → group "inventory-service"
//   dotnet run --project 13-EventDriven -- notification   → group "notification-service"
// Then:
//   curl -X POST localhost:5080/orders -H "Content-Type: application/json" -d "{\"customerId\":\"c-1\",\"productId\":\"book\",\"quantity\":2}"
//   curl -X POST localhost:5080/orders/bulk?count=500     → watch both services + 12-LagMonitor
using System.Text.Json;
using Confluent.Kafka;

const string topic = "shop.order.created";      // <domain>.<entity>.<event>
await Cluster.EnsureTopicAsync(topic, partitions: 6);

switch (args.FirstOrDefault())
{
    case "api": RunApi(); break;
    case "inventory": RunConsumer("inventory-service", e => $"📦 reserved {e.Quantity} × {e.ProductId} (order {e.OrderId})"); break;
    case "notification": RunConsumer("notification-service", e => $"📧 email to {e.CustomerId}: order {e.OrderId} confirmed"); break;
    default: Console.WriteLine("usage: dotnet run -- api | inventory | notification"); break;
}

void RunApi()
{
    var builder = WebApplication.CreateBuilder();
    builder.WebHost.UseUrls("http://localhost:5080");
    builder.Services.AddSingleton(new ProducerBuilder<string, string>(new ProducerConfig
    {
        BootstrapServers = Cluster.Bootstrap,
        Acks = Acks.All,
        EnableIdempotence = true,
        LingerMs = 5,
        Partitioner = Partitioner.Murmur2Random
    }).Build());                                   // ONE producer per app (thread-safe)

    var app = builder.Build();

    app.MapPost("/orders", async (PlaceOrder req, IProducer<string, string> producer) =>
    {
        var @event = new OrderCreated(Guid.NewGuid(), Guid.NewGuid(), req.CustomerId, req.ProductId, req.Quantity);
        var r = await producer.ProduceAsync(topic, new Message<string, string>
        {
            Key = req.CustomerId,                  // all orders of one customer stay ordered
            Value = JsonSerializer.Serialize(@event)
        });
        return Results.Ok(new { @event.OrderId, partition = r.Partition.Value, offset = r.Offset.Value });
    });

    app.MapPost("/orders/bulk", (int count, IProducer<string, string> producer) =>
    {
        for (var i = 0; i < count; i++)
        {
            var customer = $"c-{i % 20}";
            var @event = new OrderCreated(Guid.NewGuid(), Guid.NewGuid(), customer, $"p-{i % 7}", 1 + i % 3);
            producer.Produce(topic, new Message<string, string> { Key = customer, Value = JsonSerializer.Serialize(@event) });
        }
        producer.Flush(TimeSpan.FromSeconds(10));
        return Results.Ok(new { sent = count });
    });

    app.Run();
}

void RunConsumer(string groupId, Func<OrderCreated, string> handle)
{
    using var consumer = new ConsumerBuilder<string, string>(new ConsumerConfig
    {
        BootstrapServers = Cluster.Bootstrap,
        GroupId = groupId,                         // different group = its own copy of every event
        AutoOffsetReset = AutoOffsetReset.Earliest,
        EnableAutoCommit = true,                   // background commit every 5s...
        EnableAutoOffsetStore = false,             // ...of only what we StoreOffset'ed (after processing)
        PartitionAssignmentStrategy = PartitionAssignmentStrategy.CooperativeSticky
    }).Build();
    consumer.Subscribe(topic);

    var processed = new HashSet<Guid>();           // idempotent consumer (use a DB table in real life)
    var ct = Cluster.CtrlC();
    try
    {
        while (true)
        {
            var r = consumer.Consume(ct);
            var @event = JsonSerializer.Deserialize<OrderCreated>(r.Message.Value)!;

            if (processed.Add(@event.EventId))
                Console.WriteLine($"[{groupId}] p{r.Partition.Value}@{r.Offset.Value} {handle(@event)}");

            consumer.StoreOffset(r);               // after processing → at-least-once, no round-trip per record
        }
    }
    catch (OperationCanceledException) { }
    finally { consumer.Close(); }
}

record PlaceOrder(string CustomerId, string ProductId, int Quantity);
record OrderCreated(Guid EventId, Guid OrderId, string CustomerId, string ProductId, int Quantity);
