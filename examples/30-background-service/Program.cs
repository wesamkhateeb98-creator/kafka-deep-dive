// 30 - Kafka in a .NET Generic Host: singleton producer, BackgroundService consumer, graceful shutdown.
using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

var builder = Host.CreateApplicationBuilder(args);

builder.Logging.ClearProviders();

builder.Logging.AddSimpleConsole(o => { o.SingleLine = true; o.TimestampFormat = "HH:mm:ss.fff "; });

builder.Logging.AddFilter("Microsoft", LogLevel.Warning);

// one producer for the whole app: thread-safe, keeps connections and batches
builder.Services.AddSingleton(_ => new ProducerBuilder<string, string>(new ProducerConfig
{
    BootstrapServers = Lab.Bootstrap,
    EnableIdempotence = true
}).Build());

builder.Services.AddHostedService<OrderApiSimulator>();

builder.Services.AddHostedService<EmailWorker>();

// shut down by itself after 6 s so the demo ends (in real life: Ctrl+C / SIGTERM from Kubernetes)
builder.Services.AddHostedService<StopAfter>();

await builder.Build().RunAsync();

// pretends to be an API that publishes OrderPlaced events
sealed class OrderApiSimulator(IProducer<string, string> producer, ILogger<OrderApiSimulator> log) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var id = 0;

        while (!stoppingToken.IsCancellationRequested)
        {
            var key = $"order-{++id}";

            var result = await producer.ProduceAsync("30-orders", new Message<string, string> { Key = key, Value = "OrderPlaced" }, stoppingToken);

            log.LogInformation("published {Key} -> p{Partition}@{Offset}", key, result.Partition.Value, result.Offset.Value);

            await Task.Delay(1000, stoppingToken).ContinueWith(_ => { });
        }
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        await base.StopAsync(cancellationToken);

        // deliver everything still queued before the process exits
        var left = producer.Flush(TimeSpan.FromSeconds(5));

        log.LogInformation("producer flushed, {Left} message(s) left", left);
    }
}

// consumes on its own thread: Consume() blocks, so never run it on the host's startup path
sealed class EmailWorker(ILogger<EmailWorker> log) : BackgroundService
{
    protected override Task ExecuteAsync(CancellationToken stoppingToken) => Task.Factory.StartNew(() => Run(stoppingToken), TaskCreationOptions.LongRunning);

    private void Run(CancellationToken stoppingToken)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = Lab.Bootstrap,
            GroupId = "30-email-worker",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoOffsetStore = false
        };

        using var consumer = new ConsumerBuilder<string, string>(config)
            .SetPartitionsAssignedHandler((_, parts) => log.LogInformation("assigned {Count} partition(s)", parts.Count))
            .Build();

        consumer.Subscribe("30-orders");

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                var record = consumer.Consume(stoppingToken);

                log.LogInformation("sending email for {Key}", record.Message.Key);

                // mark as done only after the work succeeded; auto-commit sends it every 5 s
                consumer.StoreOffset(record);
            }
        }
        catch (OperationCanceledException)
        {
            log.LogInformation("stop requested");
        }
        finally
        {
            consumer.Close();

            log.LogInformation("consumer closed: offsets committed, left group");
        }
    }
}

sealed class StopAfter(IHostApplicationLifetime lifetime) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await Task.Delay(TimeSpan.FromSeconds(6), stoppingToken);

        lifetime.StopApplication();
    }
}

static class Lab
{
    public const string Bootstrap = "localhost:9092,localhost:9094,localhost:9096";
}
