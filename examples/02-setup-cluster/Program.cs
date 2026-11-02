// 02 - Setup: connect to the 3-node KRaft cluster and print what it looks like.
using Confluent.Kafka;
using Confluent.Kafka.Admin;

const string bootstrap = "localhost:9092,localhost:9094,localhost:9096";

using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrap }).Build();

var cluster = await admin.DescribeClusterAsync();

Console.WriteLine($"Cluster id : {cluster.ClusterId}");

Console.WriteLine($"Controller : node {cluster.Controller?.Id}");

foreach (var node in cluster.Nodes.OrderBy(n => n.Id))
{
    Console.WriteLine($"Broker     : id={node.Id} address={node.Host}:{node.Port}");
}

var metadata = admin.GetMetadata(TimeSpan.FromSeconds(10));

Console.WriteLine($"Topics     : {metadata.Topics.Count(t => !t.Topic.StartsWith("__"))} user, {metadata.Topics.Count(t => t.Topic.StartsWith("__"))} internal");
