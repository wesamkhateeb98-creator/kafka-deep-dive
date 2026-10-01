// 12 — Consumer lag = (log end offset) − (committed offset), per partition
// Run: dotnet run --project 12-LagMonitor -- demo-reader
// Tip: run 02-Producer a few times, then watch lag drop to 0 while 03-Consumer runs.
using Confluent.Kafka;
using Confluent.Kafka.Admin;

var group = args.FirstOrDefault() ?? "demo-reader";
using var admin = Cluster.Admin();
using var probe = new ConsumerBuilder<Ignore, Ignore>(new ConsumerConfig
{
    BootstrapServers = Cluster.Bootstrap,
    GroupId = "lag-probe"                     // never subscribes, only asks for watermarks
}).Build();

var ct = Cluster.CtrlC();
while (!ct.IsCancellationRequested)
{
    // committed offsets of the group (stored in __consumer_offsets)
    var committed = await admin.ListConsumerGroupOffsetsAsync([new ConsumerGroupTopicPartitions(group, null)]);

    if (!Console.IsOutputRedirected) Console.Clear();
    Console.WriteLine($"group: {group}   {DateTime.Now:HH:mm:ss}\n");
    Console.WriteLine("topic                partition  committed  end      lag");

    long total = 0;
    foreach (var p in committed[0].Partitions.OrderBy(p => p.Topic).ThenBy(p => p.Partition.Value))
    {
        var end = probe.QueryWatermarkOffsets(p.TopicPartition, TimeSpan.FromSeconds(5)).High.Value;
        var lag = p.Offset == Offset.Unset ? end : end - p.Offset.Value;
        total += lag;
        Console.WriteLine($"{p.Topic,-20} {p.Partition.Value,9}  {p.Offset.Value,9}  {end,-8} {lag}");
    }

    Console.WriteLine($"\nTOTAL LAG = {total}   (Ctrl+C to exit)");
    if (Console.IsOutputRedirected) break;   // piped / CI → print once
    try { await Task.Delay(2000, ct); } catch (OperationCanceledException) { }
}
