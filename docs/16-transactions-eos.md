# 16 · Transactions & exactly-once

**Definition:** A Kafka **transaction** makes a group of writes to many partitions — including consumer offset commits — visible **all together or not at all** to `read_committed` consumers.

**Why it matters:** "Read transfer → write ledger entry → commit offset" must not produce two ledger entries after a crash. Transactions give **exactly-once** for Kafka-to-Kafka pipelines.

## Example

Each transaction ends with a **control marker** in the log; `read_committed` consumers hide aborted records and markers.

```
16-transfers p0
offset:          0       1       2          3       4       5
                 alice   bob     COMMIT     carol   dave    ABORT
                 └─ tx 1 ─┘      marker     └─ tx 2 ─┘      marker

read_uncommitted :  alice   bob             carol   dave
read_committed   :  alice   bob
```

Consume-transform-produce: each transaction = ledger write + offset commit, so the ledger got exactly 2 entries (alice@0, bob@2; offset 1 is a marker).

## Diagram

```mermaid
sequenceDiagram
    participant P as Transactional producer (16-ledger-writer-0)
    participant TC as Transaction coordinator
    participant Out as 16-ledger partition
    participant Off as __consumer_offsets
    P->>TC: InitTransactions (fence older instances, epoch++)
    P->>TC: BeginTransaction / AddPartitions
    P->>Out: write DEBIT record (uncommitted)
    P->>TC: SendOffsetsToTransaction(input offset+1)
    P->>TC: CommitTransaction
    TC->>Out: COMMIT marker
    TC->>Off: COMMIT marker
    Note over Out,Off: read_committed consumers see record + offset together
```

| Isolation level | Sees aborted records? | Reads up to |
|---|---|---|
| `read_uncommitted` (default) | yes | high watermark |
| `read_committed` | no | last stable offset (before open transactions) |

## Code

[examples/16-transactions-eos/Program.cs](../examples/16-transactions-eos/Program.cs)

```csharp
txProducer.InitTransactions(TimeSpan.FromSeconds(10));

while (consumer.Consume(TimeSpan.FromSeconds(5)) is { } record)
{
    txProducer.BeginTransaction();

    txProducer.Produce(output, new Message<string, string> { Key = record.Message.Key, Value = $"DEBIT ..." });

    // the consumer offset is committed inside the producer's transaction
    txProducer.SendOffsetsToTransaction([new TopicPartitionOffset(record.TopicPartition, record.Offset + 1)], consumer.ConsumerGroupMetadata, TimeSpan.FromSeconds(10));

    txProducer.CommitTransaction();
}
```

Output:

```text
read_uncommitted sees: alice@0 | bob@1 | carol@3 | dave@4
read_committed   sees: alice@0 | bob@1
tx committed: in offset 0 -> ledger + offset 1
tx committed: in offset 1 -> ledger + offset 2
ledger (read_committed): alice@0 | bob@2
```

Offset 2 is the **commit marker** of tx 1, offsets 3–4 are the aborted records, offset 5 is the **abort marker**. Markers are never returned, and aborted records are hidden from `read_committed` readers — that is why offsets have gaps (the ledger shows `bob@2` because offset 1 is tx 1's marker there).

> The example calls `Flush()` before `AbortTransaction()`; without it librdkafka purges the still-queued records and they never reach the broker at all.

## ✅ Correct vs ❌ wrong usage

| | Usage | Reason |
|---|---|---|
| ✅ | Consume-transform-produce with `SendOffsetsToTransaction` | Output and input progress commit atomically |
| ✅ | Stable `TransactionalId` per logical instance (e.g. `ledger-writer-0`) | A restarted instance fences its zombie |
| ✅ | Downstream consumers use `IsolationLevel.ReadCommitted` | Otherwise they see aborted data |
| ❌ | Expect a transaction to cover a DB write or HTTP call | Only Kafka writes are atomic; use the outbox ([19](19-outbox-pattern.md)) |
| ❌ | One transaction per record at very high volume | Each commit costs round trips; batch several records per transaction |

## Pitfalls

1. Exactly-once is **Kafka → Kafka** only; side effects outside Kafka can still repeat.
2. Long-open transactions block `read_committed` consumers (LSO stops moving) — keep them short (`transaction.timeout.ms`).
3. `ProducerFencedException` means another instance took your `TransactionalId`: stop, don't retry.
4. Needs RF ≥ 3 and `transaction.state.log.min.isr=2` on the broker for safety.

**Next:** [17 · Stream processing](17-stream-processing.md)
