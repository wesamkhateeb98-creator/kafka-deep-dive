// 07 — Commit position decides the delivery semantics
//   dotnet run --project 07-CommitSemantics -- seed            → writes 10 payments (pay-1..pay-10)
//   dotnet run --project 07-CommitSemantics -- after  --crash 5 → processes 1..5, crashes BEFORE committing 5
//   dotnet run --project 07-CommitSemantics -- after            → pay-5 processed AGAIN  (at-least-once: duplicate)
//   dotnet run --project 07-CommitSemantics -- reset
//   dotnet run --project 07-CommitSemantics -- before --crash 5 → commits 5, crashes BEFORE processing it
//   dotnet run --project 07-CommitSemantics -- before           → pay-5 is NEVER processed (at-most-once: loss)
using Confluent.Kafka;

const string topic = "demo.payments";
const string group = "demo-commit";
var mode = args.FirstOrDefault() ?? "after";
var crashAt = args.SkipWhile(a => a != "--crash").Skip(1).Select(int.Parse).FirstOrDefault();

switch (mode)
{
    case "seed":
        await Cluster.EnsureTopicAsync(topic, partitions: 1);   // 1 partition → easy to follow
        using (var p = new ProducerBuilder<Null, string>(new ProducerConfig { BootstrapServers = Cluster.Bootstrap }).Build())
            for (var i = 1; i <= 10; i++) await p.ProduceAsync(topic, new Message<Null, string> { Value = $"pay-{i}" });
        Console.WriteLine("seeded pay-1..pay-10");
        return;

    case "reset":
        using (var admin = Cluster.Admin())
            await admin.DeleteGroupsAsync([group]);    // fails with NonEmptyGroup while a member is alive
        Console.WriteLine($"group '{group}' deleted → next run starts from the earliest offset");
        return;
}

using var consumer = new ConsumerBuilder<Null, string>(new ConsumerConfig
{
    BootstrapServers = Cluster.Bootstrap,
    GroupId = group,
    AutoOffsetReset = AutoOffsetReset.Earliest,
    EnableAutoCommit = false,           // we decide exactly when the offset is committed
    SessionTimeoutMs = 6_000            // a crashed member keeps its partitions until this expires (default 45s!)
}).Build();
consumer.Subscribe(topic);

// Wait for the group join (a crashed previous run may still hold the partition for ~6s), then stop after 3s idle
var idleSeconds = 0;
while (idleSeconds < 3)
{
    var r = consumer.Consume(TimeSpan.FromSeconds(1));
    if (r is null)
    {
        if (consumer.Assignment.Count > 0) idleSeconds++;
        continue;
    }
    idleSeconds = 0;

    var n = int.Parse(r.Message.Value.Split('-')[1]);

    if (mode == "before")
    {
        consumer.Commit(r);                                   // 1) commit
        if (n == crashAt) Crash(r.Message.Value, "after commit, before processing");
        Console.WriteLine($"💳 charged {r.Message.Value}");   // 2) process
    }
    else
    {
        Console.WriteLine($"💳 charged {r.Message.Value}");   // 1) process
        if (n == crashAt) Crash(r.Message.Value, "after processing, before commit");
        consumer.Commit(r);                                   // 2) commit
    }
}

consumer.Close();
Console.WriteLine("no more records");

static void Crash(string value, string where)
{
    Console.WriteLine($"💥 CRASH on {value} ({where})");
    Environment.Exit(1);                // no Close(), no commit — like a killed pod
}
