// 19 - Transactional outbox: save the order and its event in ONE database transaction,
// then a relay publishes pending outbox rows to Kafka.
using Confluent.Kafka;
using Microsoft.Data.Sqlite;

const string bootstrap = "localhost:9092,localhost:9094,localhost:9096";

const string topic = "19-order-events";

File.Delete("shop.db");

using var db = new SqliteConnection("Data Source=shop.db");

db.Open();

Execute("""
    CREATE TABLE orders (id TEXT PRIMARY KEY, customer TEXT, total REAL);
    CREATE TABLE outbox (seq INTEGER PRIMARY KEY AUTOINCREMENT, aggregate_id TEXT, type TEXT, payload TEXT, sent_at TEXT NULL);
    """);

PlaceOrder("ord-1", "alice", 120.5);

PlaceOrder("ord-2", "bob", 35.0);

try
{
    PlaceOrder("ord-3", "carol", -1);
}
catch (InvalidOperationException ex)
{
    Console.WriteLine($"ord-3 rejected: {ex.Message} -> no order row, no outbox row");
}

PrintTables("after PlaceOrder");

// relay: normally a background loop every N ms
await RelayOnce();

PrintTables("after relay");

void PlaceOrder(string id, string customer, double total)
{
    using var tx = db.BeginTransaction();

    Execute("INSERT INTO orders VALUES ($p0, $p1, $p2)", id, customer, total);

    Execute("INSERT INTO outbox (aggregate_id, type, payload) VALUES ($p0, 'OrderPlaced', $p1)", id, $$"""{"id":"{{id}}","customer":"{{customer}}","total":{{total}}}""");

    if (total <= 0)
    {
        // rollback undoes BOTH inserts: the event can never exist without the order
        tx.Rollback();

        throw new InvalidOperationException("total must be positive");
    }

    tx.Commit();

    Console.WriteLine($"{id} saved (order + outbox row in one transaction)");
}

async Task RelayOnce()
{
    var config = new ProducerConfig { BootstrapServers = bootstrap, EnableIdempotence = true };

    using var producer = new ProducerBuilder<string, string>(config).Build();

    using var select = db.CreateCommand();

    select.CommandText = "SELECT seq, aggregate_id, type, payload FROM outbox WHERE sent_at IS NULL ORDER BY seq";

    var pending = new List<(long Seq, string Key, string Type, string Payload)>();

    using (var reader = select.ExecuteReader())
    {
        while (reader.Read())
        {
            pending.Add((reader.GetInt64(0), reader.GetString(1), reader.GetString(2), reader.GetString(3)));
        }
    }

    foreach (var row in pending)
    {
        var headers = new Headers { { "type", System.Text.Encoding.UTF8.GetBytes(row.Type) }, { "outbox-seq", BitConverter.GetBytes(row.Seq) } };

        // key = aggregate id keeps all events of one order in order
        var result = await producer.ProduceAsync(topic, new Message<string, string> { Key = row.Key, Value = row.Payload, Headers = headers });

        // crash here = row stays unsent = published again later (at-least-once; consumers dedupe by outbox-seq)
        Execute("UPDATE outbox SET sent_at = $p0 WHERE seq = $p1", DateTime.UtcNow.ToString("O"), row.Seq);

        Console.WriteLine($"relay: outbox seq {row.Seq} -> {topic} p{result.Partition.Value}@{result.Offset.Value}");
    }
}

void PrintTables(string title)
{
    using var cmd = db.CreateCommand();

    cmd.CommandText = "SELECT (SELECT COUNT(*) FROM orders), (SELECT COUNT(*) FROM outbox), (SELECT COUNT(*) FROM outbox WHERE sent_at IS NULL)";

    using var r = cmd.ExecuteReader();

    r.Read();

    Console.WriteLine($"[{title}] orders={r.GetInt64(0)} outbox={r.GetInt64(1)} pending={r.GetInt64(2)}");
}

void Execute(string sql, params object[] values)
{
    using var cmd = db.CreateCommand();

    cmd.CommandText = sql;

    for (var i = 0; i < values.Length; i++)
    {
        cmd.Parameters.AddWithValue($"$p{i}", values[i]);
    }

    cmd.ExecuteNonQuery();
}
