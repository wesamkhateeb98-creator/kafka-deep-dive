// 31 - Change data capture with Debezium: every INSERT/UPDATE/DELETE in Postgres becomes a Kafka event.
// Needs: ./connect/get-debezium.sh && docker compose --profile cdc up -d
using System.Net.Http.Json;
using System.Text.Json;
using Confluent.Kafka;
using Npgsql;

const string bootstrap = "localhost:9092,localhost:9094,localhost:9096";

const string connector = "31-shop-cdc";

const string topic = "31-shop.public.customers";

await using var db = NpgsqlDataSource.Create("Host=localhost;Username=shop;Password=shop;Database=shop");

await Sql("DROP TABLE IF EXISTS customers");

await Sql("CREATE TABLE customers (id INT PRIMARY KEY, name TEXT, city TEXT)");

// FULL = the "before" image contains all columns, not just the key
await Sql("ALTER TABLE customers REPLICA IDENTITY FULL");

await Sql("INSERT INTO customers VALUES (1, 'alice', 'Berlin')");

using var http = new HttpClient { BaseAddress = new Uri("http://localhost:8083") };

await http.DeleteAsync($"/connectors/{connector}");

var body = new
{
    name = connector,
    config = new Dictionary<string, string>
    {
        ["connector.class"] = "io.debezium.connector.postgresql.PostgresConnector",
        ["database.hostname"] = "postgres",
        ["database.port"] = "5432",
        ["database.user"] = "shop",
        ["database.password"] = "shop",
        ["database.dbname"] = "shop",
        ["plugin.name"] = "pgoutput",
        ["slot.name"] = "slot_31",
        ["topic.prefix"] = "31-shop",
        ["table.include.list"] = "public.customers",
        ["key.converter"] = "org.apache.kafka.connect.json.JsonConverter",
        ["key.converter.schemas.enable"] = "false",
        ["value.converter"] = "org.apache.kafka.connect.json.JsonConverter",
        ["value.converter.schemas.enable"] = "false"
    }
};

var created = await http.PostAsJsonAsync("/connectors", body);

Console.WriteLine($"POST /connectors -> {(int)created.StatusCode}");

await Task.Delay(TimeSpan.FromSeconds(8));

// changes made AFTER the initial snapshot are streamed from the WAL
await Sql("INSERT INTO customers VALUES (2, 'bob', 'Paris')");

await Sql("UPDATE customers SET city = 'Munich' WHERE id = 1");

await Sql("DELETE FROM customers WHERE id = 2");

await Task.Delay(TimeSpan.FromSeconds(5));

using var consumer = new ConsumerBuilder<string, string?>(new ConsumerConfig { BootstrapServers = bootstrap, GroupId = "31-reader" }).Build();

consumer.Assign(Enumerable.Range(0, 3).Select(p => new TopicPartitionOffset(topic, p, Offset.Beginning)));

var events = new List<ConsumeResult<string, string?>>();

while (consumer.Consume(TimeSpan.FromSeconds(3)) is { } r)
{
    events.Add(r);
}

foreach (var r in events.OrderBy(e => e.Message.Timestamp.UnixTimestampMs).ThenBy(e => e.Offset.Value))
{
    if (r.Message.Value is null)
    {
        Console.WriteLine($"key={r.Message.Key} -> tombstone (lets compaction drop the key)");

        continue;
    }

    var e = JsonDocument.Parse(r.Message.Value).RootElement;

    var op = e.GetProperty("op").GetString() switch { "r" => "r (snapshot)", "c" => "c (insert)", "u" => "u (update)", "d" => "d (delete)", var x => x };

    Console.WriteLine($"key={r.Message.Key} op={op,-12} before={Row(e, "before")} after={Row(e, "after")}");
}

await http.DeleteAsync($"/connectors/{connector}");

await Sql("SELECT pg_drop_replication_slot('slot_31') WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = 'slot_31' AND NOT active)");

async Task Sql(string sql)
{
    await using var cmd = db.CreateCommand(sql);

    await cmd.ExecuteNonQueryAsync();
}

static string Row(JsonElement e, string name) =>
    e.GetProperty(name).ValueKind == JsonValueKind.Null ? "null" : e.GetProperty(name).GetRawText();
