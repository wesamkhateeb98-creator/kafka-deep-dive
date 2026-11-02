// 15 - KRaft: who is the active controller, and what roles does each node play?
using Confluent.Kafka;
using Confluent.Kafka.Admin;

const string bootstrap = "localhost:9092,localhost:9094,localhost:9096";

using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrap }).Build();

var cluster = await admin.DescribeClusterAsync();

Console.WriteLine($"cluster.id         = {cluster.ClusterId}");

// in KRaft, brokers answer this with a RANDOM broker id (clients never talk to controllers directly);
// the real Raft leader comes from: kafka-metadata-quorum.sh describe --status
Console.WriteLine($"DescribeCluster    = controller node {cluster.Controller?.Id} (random broker in KRaft)");

var configs = new List<DescribeConfigsResult>();

foreach (var node in cluster.Nodes.OrderBy(n => n.Id))
{
    // only one BROKER resource is allowed per DescribeConfigs call
    var result = await admin.DescribeConfigsAsync([new ConfigResource { Type = ResourceType.Broker, Name = node.Id.ToString() }]);

    configs.Add(result[0]);
}

string[] keys = ["node.id", "process.roles", "controller.quorum.voters", "controller.listener.names", "advertised.listeners"];

foreach (var node in configs)
{
    Console.WriteLine();

    Console.WriteLine($"node {node.ConfigResource.Name}:");

    foreach (var key in keys)
    {
        var value = node.Entries.TryGetValue(key, out var entry) ? entry.Value ?? "(null)" : "(n/a)";

        Console.WriteLine($"  {key,-26} = {value}");
    }
}
