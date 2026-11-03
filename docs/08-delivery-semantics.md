# 08 · Delivery semantics (producer side)

**Definition:** **Delivery semantics** say how many times an event can end up in Kafka when things fail: *at-most-once* (0–1), *at-least-once* (1–n), *exactly-once* (1).

**Why it matters:** A network timeout after the broker stored a batch makes the producer retry → duplicate. `acks` and **idempotence** decide whether you lose or duplicate data.

## Example

500 awaited sends per setting, topic RF 3, `min.insync.replicas=2`:

| `acks` | Who must store it before the ack | Status | ms / msg | Loss possible when |
|---|---|---|---|---|
| 0 | nobody | PossiblyPersisted (no offset) | 5.502 | always (fire and forget) |
| 1 | leader | Persisted | 7.066 | leader dies before followers copy |
| all | leader + ISR (≥ 2) | Persisted | 9.489 | never (while ISR ≥ 2) |
| all + idempotent | leader + ISR, dedupe on retry | Persisted | 8.646 | never, and no duplicates |

Cost of safety: 9.489 − 5.502 = **+3.987 ms** per awaited message; idempotence adds nothing measurable.

## Diagram

```mermaid
sequenceDiagram
    participant P as Producer (PID=7)
    participant L as Leader
    participant F as Follower
    P->>L: batch seq=5
    L->>F: replicate
    F-->>L: ok
    L--xP: ack lost (network)
    Note over P: timeout → retry
    P->>L: batch seq=5 again
    alt enable.idempotence=false
        L-->>P: stored AGAIN → duplicate
    else enable.idempotence=true
        L-->>P: seq 5 already seen for PID 7 → ack, no duplicate
    end
```

| `acks` | Leader waits for | Loses data when | Latency |
|---|---|---|---|
| `0` | nothing (no response) | network drop, broker crash | lowest |
| `1` | its own disk write | leader dies before followers copy | low |
| `all` (`-1`) | all **in-sync** replicas (≥ `min.insync.replicas`) | all ISR die at once | higher |

Idempotent producer = `acks=all` + retries + `max.in.flight ≤ 5` + producer id/sequence numbers per partition.

## Code

[examples/08-delivery-semantics/Program.cs](../examples/08-delivery-semantics/Program.cs)

```csharp
var config = new ProducerConfig { BootstrapServers = bootstrap, Acks = acks, EnableIdempotence = idempotent };

using var producer = new ProducerBuilder<string, string>(config).Build();

for (var i = 0; i < messages; i++)
{
    await producer.ProduceAsync(topic, new Message<string, string> { Key = "sensor-1", Value = $"{20 + i % 10}.0" });
}
```

Output:

```text
acks    | idempotent | first result                     | avg latency (awaited, 500 msgs)
--------+------------+----------------------------------+--------------------------------
None    | False      | status=PossiblyPersisted offset=unknown | 5.502 ms
Leader  | False      | status=Persisted         offset=2505    | 7.066 ms
All     | False      | status=Persisted         offset=3006    | 9.489 ms
All     | True       | status=Persisted         offset=3507    | 8.646 ms
```

Note `acks=0`: status is `PossiblyPersisted` and the offset is unknown — the producer never learns whether the broker stored it. The idempotent run is not slower than plain `acks=all` (8.646 vs 9.489 ms): the difference is noise.

## ✅ Correct vs ❌ wrong usage

| | Usage | Reason |
|---|---|---|
| ✅ | `EnableIdempotence = true` for anything business-critical | No duplicates or reordering from retries, tiny cost |
| ✅ | `acks=all` + topic `min.insync.replicas=2` + RF=3 | Survives 1 broker loss with no data loss |
| ✅ | `acks=0` for metrics/telemetry you can afford to lose | Lowest latency |
| ❌ | `acks=all` with `min.insync.replicas=1` | "all" may mean just the leader → data loss on failover |
| ❌ | Thinking idempotence de-duplicates across app restarts | It covers retries of one producer session only; use transactions or consumer-side dedupe |

## Pitfalls

1. Idempotence removes **broker-side** duplicates from retries, not duplicates your app creates by calling `Produce` twice.
2. `message.timeout.ms` expiry = message **failed**; you must handle the error (retry, log, DLQ).
3. `acks=all` latency is bound by the slowest in-sync follower.
4. Exactly-once end-to-end needs transactions + `read_committed` consumers (see [16](16-transactions-eos.md)).

**Next:** [09 · Offset commits](09-offset-commits.md)
