// 03 — Consumer: the poll loop
// Run: dotnet run --project 03-Consumer          (Ctrl+C to stop)
// Run again → continues from the committed offset (nothing re-printed)
// Change GroupId → a new group starts from the beginning (AutoOffsetReset = Earliest)
using System.Text;
using Confluent.Kafka;

var config = new ConsumerConfig
{
    BootstrapServers = Cluster.Bootstrap,
    GroupId = "demo-reader",                 // offsets are stored per group
    AutoOffsetReset = AutoOffsetReset.Earliest, // no committed offset yet → start from the oldest record
    EnableAutoCommit = true,                 // commit in the background every 5s (auto.commit.interval.ms)
};

using var consumer = new ConsumerBuilder<string, string>(config).Build();
consumer.Subscribe("demo.orders");

var ct = Cluster.CtrlC();
var count = 0;
try
{
    while (true)
    {
        var r = consumer.Consume(ct);        // returns 1 record from the locally fetched batch
        var type = r.Message.Headers.TryGetLastBytes("event-type", out var b) ? Encoding.UTF8.GetString(b) : "-";

        Console.WriteLine($"p{r.Partition.Value} @{r.Offset.Value,-5} key={r.Message.Key,-12} type={type,-17} {r.Message.Value}");
        count++;
    }
}
catch (OperationCanceledException) { }
finally
{
    consumer.Close();                        // commit final offsets + leave group → instant rebalance
    Console.WriteLine($"\nconsumed {count} records");
}
