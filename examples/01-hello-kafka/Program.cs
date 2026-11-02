// 01 - Hello Kafka: write one event, read it back.
using Confluent.Kafka;

const string bootstrap = "localhost:9092,localhost:9094,localhost:9096";

const string topic = "01-hello";

using var producer = new ProducerBuilder<string, string>(new ProducerConfig { BootstrapServers = bootstrap }).Build();

// the topic is auto-created on first write (auto.create.topics.enable=true by default)
var delivered = await producer.ProduceAsync(topic, new Message<string, string> { Key = "greeting", Value = "Hello, Kafka!" });

Console.WriteLine($"Produced -> topic={delivered.Topic} partition={delivered.Partition.Value} offset={delivered.Offset.Value}");

var consumerConfig = new ConsumerConfig
{
    BootstrapServers = bootstrap,
    GroupId = "01-hello-reader",
    AutoOffsetReset = AutoOffsetReset.Earliest
};

using var consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();

// read exactly the record we just wrote
consumer.Assign(delivered.TopicPartitionOffset);

var record = consumer.Consume(TimeSpan.FromSeconds(10));

Console.WriteLine($"Consumed <- topic={record.Topic} partition={record.Partition.Value} offset={record.Offset.Value} key={record.Message.Key} value={record.Message.Value}");

consumer.Close();
