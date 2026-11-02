// 21 - Security: SASL authentication + ACL authorization (needs: docker compose --profile security up -d).
using Confluent.Kafka;
using Confluent.Kafka.Admin;

const string bootstrap = "localhost:9192";

const string topic = "21-salaries";

// demo credentials from docker-compose.yml; in real life load them from a secret store
ClientConfig As(string user, string password) => new()
{
    BootstrapServers = bootstrap,
    SecurityProtocol = SecurityProtocol.SaslPlaintext,
    SaslMechanism = SaslMechanism.Plain,
    SaslUsername = user,
    SaslPassword = password
};

using (var admin = new AdminClientBuilder(new AdminClientConfig(As("admin", "admin-secret"))).Build())
{
    try
    {
        await admin.CreateTopicsAsync([new TopicSpecification { Name = topic, NumPartitions = 1, ReplicationFactor = 1 }]);
    }
    catch (CreateTopicsException) { }

    // alice may WRITE to the topic; bob gets nothing
    var allowAliceWrite = new AclBinding
    {
        Pattern = new ResourcePattern { Type = ResourceType.Topic, Name = topic, ResourcePatternType = ResourcePatternType.Literal },
        Entry = new AccessControlEntry { Principal = "User:alice", Host = "*", Operation = AclOperation.Write, PermissionType = AclPermissionType.Allow }
    };

    await admin.CreateAclsAsync([allowAliceWrite]);

    var acls = await admin.DescribeAclsAsync(new AclBindingFilter
    {
        PatternFilter = new ResourcePatternFilter { Type = ResourceType.Topic, Name = topic, ResourcePatternType = ResourcePatternType.Any },
        EntryFilter = new AccessControlEntryFilter { Operation = AclOperation.Any, PermissionType = AclPermissionType.Any }
    });

    foreach (var acl in acls.AclBindings)
    {
        Console.WriteLine($"ACL: {acl.Entry.Principal} {acl.Entry.PermissionType} {acl.Entry.Operation} on {acl.Pattern.Type}:{acl.Pattern.Name}");
    }
}

Console.WriteLine();

await TryWrite("alice", "alice-secret");

await TryWrite("bob", "bob-secret");

await TryWrite("bob", "wrong-password");

async Task TryWrite(string user, string password)
{
    var config = new ProducerConfig(As(user, password)) { MessageTimeoutMs = 5000 };

    // authentication failures arrive on the error handler, not on the delivery result
    ErrorCode? authError = null;

    using var producer = new ProducerBuilder<string, string>(config)
        .SetErrorHandler((_, e) => authError ??= e.Code == ErrorCode.Local_Authentication ? e.Code : null)
        .SetLogHandler((_, _) => { })
        .Build();

    try
    {
        var result = await producer.ProduceAsync(topic, new Message<string, string> { Key = "emp-1", Value = "5000" });

        Console.WriteLine($"{user,-5} ({password,-14}) write -> OK offset {result.Offset.Value}");
    }
    catch (ProduceException<string, string> ex)
    {
        Console.WriteLine($"{user,-5} ({password,-14}) write -> DENIED {authError ?? ex.Error.Code}");
    }
}
