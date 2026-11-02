// 26 - Group coordinator: which broker manages a group, and where its offsets live (__consumer_offsets).
using Confluent.Kafka;
using Confluent.Kafka.Admin;

const string bootstrap = "localhost:9092,localhost:9094,localhost:9096";

const string topic = "26-invoices";

const string group = "26-billing";

using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrap }).Build();

try
{
    await admin.DeleteTopicsAsync([topic]);

    await Task.Delay(1000);

    await admin.DeleteGroupsAsync([group]);
}
catch (KafkaException) { }

await admin.CreateTopicsAsync([new TopicSpecification { Name = topic, NumPartitions = 4, ReplicationFactor = 3 }]);

await Task.Delay(500);

// 1. the coordinator is the leader of __consumer_offsets partition = abs(javaHashCode(groupId)) % 50
var offsetsPartition = (JavaHashCode(group) & 0x7fffffff) % 50;

var internalTopic = await admin.DescribeTopicsAsync(TopicCollection.OfTopicNames(["__consumer_offsets"]));

var leader = internalTopic.TopicDescriptions[0].Partitions.Single(p => p.Partition == offsetsPartition).Leader.Id;

Console.WriteLine($"group \"{group}\": javaHashCode={JavaHashCode(group)} -> __consumer_offsets partition {offsetsPartition} -> leader broker {leader}");

// 2. two live members, so the group has something to describe
using var stop = new CancellationTokenSource();

var members = new[] { "billing-a", "billing-b" }.Select(name => Task.Run(() =>
{
    var config = new ConsumerConfig { BootstrapServers = bootstrap, GroupId = group, ClientId = name, AutoOffsetReset = AutoOffsetReset.Earliest };

    using var consumer = new ConsumerBuilder<Ignore, Ignore>(config).Build();

    consumer.Subscribe(topic);

    while (!stop.IsCancellationRequested)
    {
        consumer.Consume(TimeSpan.FromMilliseconds(200));
    }

    consumer.Close();
})).ToArray();

await Task.Delay(TimeSpan.FromSeconds(8));

var described = await admin.DescribeConsumerGroupsAsync([group]);

var g = described.ConsumerGroupDescriptions[0];

Console.WriteLine($"DescribeConsumerGroups: state={g.State} assignor={g.PartitionAssignor} coordinator=broker {g.Coordinator.Id}");

foreach (var m in g.Members.OrderBy(m => m.ClientId))
{
    Console.WriteLine($"  member {m.ClientId,-10} owns partitions [{string.Join(",", m.Assignment.TopicPartitions.Select(tp => tp.Partition.Value).Order())}]");
}

Console.WriteLine($"coordinator == leader of __consumer_offsets-{offsetsPartition}? {g.Coordinator.Id == leader}");

stop.Cancel();

await Task.WhenAll(members);

static int JavaHashCode(string s)
{
    var h = 0;

    foreach (var c in s)
    {
        h = unchecked(31 * h + c);
    }

    return h;
}
