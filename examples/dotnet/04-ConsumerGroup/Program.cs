// 04 — Consumer Group + Rebalancing
// Open 3 terminals:
//   dotnet run --project 04-ConsumerGroup -- A
//   dotnet run --project 04-ConsumerGroup -- B
//   dotnet run --project 04-ConsumerGroup -- C
// Watch 6 partitions move: A=6 → A=3,B=3 → A=2,B=2,C=2. Ctrl+C one of them → its partitions move back.
// Flags:
//   --eager   → Range assignor: EVERY member drops ALL partitions on each rebalance (stop-the-world)
//   --static  → static membership: restart within session.timeout → no rebalance at all
using Confluent.Kafka;

var member = args.FirstOrDefault(a => !a.StartsWith("--")) ?? "A";
var eager = args.Contains("--eager");
var isStatic = args.Contains("--static");

var config = new ConsumerConfig
{
    BootstrapServers = Cluster.Bootstrap,
    GroupId = "demo-group",
    ClientId = $"member-{member}",
    AutoOffsetReset = AutoOffsetReset.Earliest,
    PartitionAssignmentStrategy = eager
        ? PartitionAssignmentStrategy.Range
        : PartitionAssignmentStrategy.CooperativeSticky,   // incremental: only moved partitions are revoked
    GroupInstanceId = isStatic ? $"static-{member}" : null,
    SessionTimeoutMs = 10_000,                             // no heartbeat for 10s → member considered dead
    HeartbeatIntervalMs = 3_000
};

var owned = new SortedSet<int>();
void Show(string what) => Console.WriteLine($"[{member}] {what,-30} → now owns: [{string.Join(",", owned)}]");

using var consumer = new ConsumerBuilder<string, string>(config)
    .SetPartitionsAssignedHandler((_, parts) =>
    {
        if (parts.Count == 0) return;   // cooperative protocol also fires with empty increments
        foreach (var p in parts) owned.Add(p.Partition.Value);
        Show($"➕ assigned {string.Join(",", parts.Select(p => p.Partition.Value))}");
    })
    .SetPartitionsRevokedHandler((_, parts) =>
    {
        // last chance to commit work of these partitions before another member takes them
        foreach (var p in parts) owned.Remove(p.Partition.Value);
        Show($"➖ revoked {string.Join(",", parts.Select(p => p.Partition.Value))}");
    })
    .SetPartitionsLostHandler((_, parts) =>
    {
        foreach (var p in parts) owned.Remove(p.Partition.Value);
        Show("❌ lost (session expired)");
    })
    .Build();

await Cluster.EnsureTopicAsync("demo.orders", partitions: 6);
consumer.Subscribe("demo.orders");
Console.WriteLine($"[{member}] joined demo-group ({(eager ? "eager/range" : "cooperative-sticky")}{(isStatic ? ", static" : "")})");

var ct = Cluster.CtrlC();
try
{
    while (true)
    {
        var r = consumer.Consume(ct);
        Console.WriteLine($"[{member}] p{r.Partition.Value}@{r.Offset.Value} {r.Message.Key}");
    }
}
catch (OperationCanceledException) { }
finally
{
    consumer.Close();   // LeaveGroup → others rebalance immediately (static members don't send it)
}
