// 01 — Admin: cluster, controller, topics, partitions, leaders, replicas, ISR
// Run: dotnet run --project 01-Admin
using Confluent.Kafka;
using Confluent.Kafka.Admin;

using var admin = Cluster.Admin();

// 1) Cluster: who are the brokers, who is the active controller (KRaft)
var cluster = await admin.DescribeClusterAsync();
Console.WriteLine($"Cluster id : {cluster.ClusterId}");
Console.WriteLine($"Controller : node {cluster.Controller?.Id}");
foreach (var node in cluster.Nodes)
    Console.WriteLine($"Broker     : id={node.Id} {node.Host}:{node.Port}");

// 2) Create a topic: 6 partitions × RF 3 = 18 partition replicas spread over 3 brokers
const string topic = "demo.orders";
await Cluster.EnsureTopicAsync(topic, partitions: 6);

// 3) Describe it: every partition has 1 leader + followers; ISR = replicas that are caught up
var described = await admin.DescribeTopicsAsync(TopicCollection.OfTopicNames([topic]));
Console.WriteLine($"\n{topic}");
Console.WriteLine("partition | leader | replicas | ISR");
foreach (var p in described.TopicDescriptions[0].Partitions)
{
    var replicas = string.Join(",", p.Replicas.Select(r => r.Id));
    var isr = string.Join(",", p.ISR.Select(r => r.Id));
    Console.WriteLine($"    {p.Partition}     |   {p.Leader?.Id}    |  {replicas}   | {isr}");
}

// 4) Topic configs (defaults + overrides)
var configs = await admin.DescribeConfigsAsync([new ConfigResource { Type = ResourceType.Topic, Name = topic }]);
foreach (var key in new[] { "retention.ms", "cleanup.policy", "min.insync.replicas", "segment.bytes" })
    Console.WriteLine($"{key,-22} = {configs[0].Entries[key].Value}");

// 5) All topics (internal ones start with "__": __consumer_offsets, __transaction_state)
var metadata = admin.GetMetadata(TimeSpan.FromSeconds(5));
Console.WriteLine("\nTopics: " + string.Join(", ", metadata.Topics.Select(t => $"{t.Topic}({t.Partitions.Count})")));
