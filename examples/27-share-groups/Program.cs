// 27 - Share groups (KIP-932, "queues for Kafka"): many workers share ONE partition.
// Needs: docker exec kafka-1 /opt/kafka/bin/kafka-features.sh --bootstrap-server localhost:9092 upgrade --feature share.version=1
using System.Diagnostics;
using Confluent.Kafka;
using Confluent.Kafka.Admin;

const string bootstrap = "localhost:9092,localhost:9094,localhost:9096";

const string topic = "27-jobs";

using (var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrap }).Build())
{
    try
    {
        await admin.DeleteTopicsAsync([topic]);

        await Task.Delay(1000);
    }
    catch (DeleteTopicsException) { }

    // ONE partition: a classic group can use only one consumer here
    await admin.CreateTopicsAsync([new TopicSpecification { Name = topic, NumPartitions = 1, ReplicationFactor = 3 }]);

    await Task.Delay(500);
}

using (var producer = new ProducerBuilder<Null, string>(new ProducerConfig { BootstrapServers = bootstrap }).Build())
{
    // awaited one by one: each job is its own batch, so the broker can hand batches to different workers
    for (var i = 1; i <= 12; i++)
    {
        await producer.ProduceAsync(topic, new Message<Null, string> { Value = $"job-{i:00}" });
    }
}

Console.WriteLine("== classic consumer group, 2 consumers, 1 partition ==");

var classicGroup = $"27-classic-{DateTime.UtcNow:HHmmss}";

var classic = await Task.WhenAll(new[] { "worker-1", "worker-2" }.Select(name => Task.Run(() =>
{
    var config = new ConsumerConfig { BootstrapServers = bootstrap, GroupId = classicGroup, ClientId = name, AutoOffsetReset = AutoOffsetReset.Earliest };

    using var consumer = new ConsumerBuilder<Null, string>(config).Build();

    consumer.Subscribe(topic);

    var got = new List<string>();

    var deadline = DateTime.UtcNow.AddSeconds(10);

    while (DateTime.UtcNow < deadline)
    {
        if (consumer.Consume(TimeSpan.FromMilliseconds(200)) is { } r)
        {
            got.Add(r.Message.Value);
        }
    }

    consumer.Close();

    return (name, got);
})));

foreach (var (name, got) in classic)
{
    Console.WriteLine($"  {name}: {got.Count,2} jobs  {string.Join(" ", got)}");
}

// librdkafka (and so Confluent.Kafka) has no share consumer yet: run the Java one inside two brokers
Console.WriteLine("== share group 27-workers, 2 share consumers, same 1 partition ==");

// both workers join first (JVM start takes a few seconds), then jobs arrive while they run
var running = new[] { "kafka-1", "kafka-2" }.Select(host => Task.Run(() =>
{
    var output = Docker(host, $"/opt/kafka/bin/kafka-console-share-consumer.sh --bootstrap-server localhost:29092 --topic {topic} --group 27-workers --timeout-ms 6000 --consumer-property max.poll.records=2");

    var jobs = output.Split('\n', StringSplitOptions.RemoveEmptyEntries).Where(l => l.StartsWith("task-")).ToList();

    return (host, jobs);
})).ToArray();

await Task.Delay(TimeSpan.FromSeconds(8));

using (var producer = new ProducerBuilder<Null, string>(new ProducerConfig { BootstrapServers = bootstrap }).Build())
{
    for (var i = 1; i <= 12; i++)
    {
        await producer.ProduceAsync(topic, new Message<Null, string> { Value = $"task-{i:00}" });

        await Task.Delay(250);
    }
}

var workers = await Task.WhenAll(running);

foreach (var (host, jobs) in workers)
{
    Console.WriteLine($"  share consumer in {host}: {jobs.Count,2} tasks  {string.Join(" ", jobs)}");
}

static string Docker(string container, string command)
{
    var info = new ProcessStartInfo("docker", $"exec {container} {command}") { RedirectStandardOutput = true, RedirectStandardError = true };

    using var process = Process.Start(info)!;

    var stdout = process.StandardOutput.ReadToEnd();

    process.WaitForExit();

    return stdout;
}
