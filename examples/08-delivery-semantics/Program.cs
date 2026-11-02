// 08 - Delivery semantics on the producer side: acks=0 / 1 / all, and the idempotent producer.
using System.Diagnostics;
using Confluent.Kafka;

const string bootstrap = "localhost:9092,localhost:9094,localhost:9096";

const string topic = "08-sensor-readings";

const int messages = 500;

Console.WriteLine("acks    | idempotent | first result                     | avg latency (awaited, 500 msgs)");

Console.WriteLine("--------+------------+----------------------------------+--------------------------------");

await Measure(Acks.None, idempotent: false);

await Measure(Acks.Leader, idempotent: false);

await Measure(Acks.All, idempotent: false);

await Measure(Acks.All, idempotent: true);

async Task Measure(Acks acks, bool idempotent)
{
    var config = new ProducerConfig { BootstrapServers = bootstrap, Acks = acks, EnableIdempotence = idempotent };

    using var producer = new ProducerBuilder<string, string>(config).Build();

    // warm-up: open connections and fetch metadata before timing
    var first = await producer.ProduceAsync(topic, new Message<string, string> { Key = "sensor-1", Value = "21.5" });

    var clock = Stopwatch.StartNew();

    for (var i = 0; i < messages; i++)
    {
        await producer.ProduceAsync(topic, new Message<string, string> { Key = "sensor-1", Value = $"{20 + i % 10}.0" });
    }

    var avg = clock.Elapsed.TotalMilliseconds / messages;

    var offset = first.Offset == Offset.Unset ? "unknown" : first.Offset.Value.ToString();

    Console.WriteLine($"{acks,-7} | {idempotent,-10} | status={first.Status,-17} offset={offset,-7} | {avg:0.000} ms");
}
