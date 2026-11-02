// 24 - Production checklist: lint every topic in the cluster against safe defaults.
using Confluent.Kafka;
using Confluent.Kafka.Admin;

const string bootstrap = "localhost:9092,localhost:9094,localhost:9096";

using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrap }).Build();

// add one deliberately unsafe topic so the linter has something to report
try
{
    var risky = new TopicSpecification
    {
        Name = "24-risky-topic",
        NumPartitions = 1,
        ReplicationFactor = 1,
        Configs = new() { ["unclean.leader.election.enable"] = "true" }
    };

    await admin.CreateTopicsAsync([risky]);

    await Task.Delay(500);
}
catch (CreateTopicsException) { }

var metadata = admin.GetMetadata(TimeSpan.FromSeconds(10));

var topics = metadata.Topics.Where(t => !t.Topic.StartsWith("__") && !t.Topic.StartsWith("_")).OrderBy(t => t.Topic).ToList();

var configs = await admin.DescribeConfigsAsync(topics.Select(t => new ConfigResource { Type = ResourceType.Topic, Name = t.Topic }));

var problems = 0;

foreach (var (topic, config) in topics.Zip(configs))
{
    var rf = topic.Partitions[0].Replicas.Length;

    var minIsr = int.Parse(config.Entries["min.insync.replicas"].Value);

    var unclean = config.Entries["unclean.leader.election.enable"].Value == "true";

    var issues = new List<string>();

    if (rf < 3)
    {
        issues.Add($"replication.factor={rf} (want >= 3)");
    }

    if (minIsr > rf)
    {
        issues.Add($"min.insync.replicas={minIsr} > rf (acks=all writes always fail)");
    }
    else if (rf >= 3 && (minIsr < 2 || minIsr == rf))
    {
        issues.Add($"min.insync.replicas={minIsr} (want 2 with RF=3)");
    }

    if (unclean)
    {
        issues.Add("unclean.leader.election.enable=true (data loss on failover)");
    }

    problems += issues.Count;

    Console.WriteLine($"{(issues.Count == 0 ? "OK  " : "WARN")} {topic.Topic,-26} partitions={topic.Partitions.Count,-2} rf={rf} minIsr={minIsr} {string.Join("; ", issues)}");
}

Console.WriteLine();

Console.WriteLine($"{topics.Count} topics checked, {problems} problem(s) found");
