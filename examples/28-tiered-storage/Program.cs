// 28 - Tiered storage (KIP-405): the broker must enable it; then old segments move to object storage.
using Confluent.Kafka;
using Confluent.Kafka.Admin;

const string bootstrap = "localhost:9092,localhost:9094,localhost:9096";

using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrap }).Build();

var spec = new TopicSpecification
{
    Name = "28-audit-log",
    NumPartitions = 3,
    ReplicationFactor = 3,
    Configs = new()
    {
        ["remote.storage.enable"] = "true",
        ["local.retention.ms"] = "86400000",     // 1 day on broker disks
        ["retention.ms"] = "2592000000"          // 30 days in total (local + remote)
    }
};

try
{
    await admin.CreateTopicsAsync([spec]);

    Console.WriteLine("topic created with tiered storage");
}
catch (CreateTopicsException ex)
{
    // this lab broker has remote.log.storage.system.enable=false (no remote storage plugin installed)
    Console.WriteLine($"create 28-audit-log -> {ex.Results[0].Error.Code}: {ex.Results[0].Error.Reason}");
}

// capacity math: 50 MB/s ingest, 30 days retention, RF 3
const double mbPerSecond = 50;

const double secondsPerDay = 86_400;

const int rf = 3;

var perDayTb = mbPerSecond * secondsPerDay / 1_000_000;

var withoutTiering = perDayTb * 30 * rf;

var localWithTiering = perDayTb * 1 * rf;

// object storage keeps ONE logical copy (it replicates internally)
var remoteWithTiering = perDayTb * 30;

Console.WriteLine();

Console.WriteLine($"ingest per day                 : {perDayTb:0.00} TB");

Console.WriteLine($"broker disk, no tiering        : {perDayTb:0.00} TB x 30 days x RF {rf} = {withoutTiering:0.0} TB");

Console.WriteLine($"broker disk, tiered (1 day)    : {perDayTb:0.00} TB x 1 day x RF {rf}  = {localWithTiering:0.00} TB");

Console.WriteLine($"object storage, tiered         : {perDayTb:0.00} TB x 30 days x 1   = {remoteWithTiering:0.0} TB");

Console.WriteLine($"broker disk saved              : {(1 - localWithTiering / withoutTiering) * 100:0.0} %");
