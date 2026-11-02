// 18 - Kafka Connect: create a file source connector over REST, then read what it imported
// (needs: docker compose --profile connect up -d).
using System.Net.Http.Json;
using System.Text.Json;
using Confluent.Kafka;

const string bootstrap = "localhost:9092,localhost:9094,localhost:9096";

const string topic = "18-legacy-orders";

const string connector = "18-legacy-orders-file";

using var http = new HttpClient { BaseAddress = new Uri("http://localhost:8083") };

var plugins = await http.GetFromJsonAsync<JsonElement[]>("/connector-plugins");

Console.WriteLine($"plugins: {string.Join(", ", plugins!.Select(p => p.GetProperty("class").GetString()!.Split('.').Last()))}");

// a legacy system drops orders into a CSV file; Connect turns each line into an event
var file = Path.GetFullPath("../../connect/data/legacy-orders.csv");

await File.WriteAllLinesAsync(file, ["1001,alice,120.50", "1002,bob,35.00", "1003,carol,980.00"]);

await http.DeleteAsync($"/connectors/{connector}");

var body = new
{
    name = connector,
    config = new Dictionary<string, string>
    {
        ["connector.class"] = "org.apache.kafka.connect.file.FileStreamSourceConnector",
        ["tasks.max"] = "1",
        ["file"] = "/connect/data/legacy-orders.csv",
        ["topic"] = topic
    }
};

var created = await http.PostAsJsonAsync("/connectors", body);

Console.WriteLine($"POST /connectors -> {(int)created.StatusCode} {created.StatusCode}");

await Task.Delay(TimeSpan.FromSeconds(5));

var status = await http.GetFromJsonAsync<JsonElement>($"/connectors/{connector}/status");

Console.WriteLine($"connector state={status.GetProperty("connector").GetProperty("state")} task0={status.GetProperty("tasks")[0].GetProperty("state")}");

using var consumer = new ConsumerBuilder<Ignore, string>(new ConsumerConfig { BootstrapServers = bootstrap, GroupId = "18-reader" }).Build();

consumer.Assign(Enumerable.Range(0, 3).Select(p => new TopicPartitionOffset(topic, p, Offset.Beginning)));

while (consumer.Consume(TimeSpan.FromSeconds(3)) is { } record)
{
    Console.WriteLine($"event p{record.Partition.Value}@{record.Offset.Value}: {record.Message.Value}");
}
