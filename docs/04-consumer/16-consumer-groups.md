# 16 · Consumer Groups

> **Group** = consumers بنفس `group.id` بيتقاسموا الـ partitions. كل partition ⇒ **member واحد بس** داخل الـ group.

```mermaid
flowchart LR
    subgraph T["topic (4 partitions)"]
        P0["p0"]
        P1["p1"]
        P2["p2"]
        P3["p3"]
    end
    subgraph G1["group: inventory-service"]
        A["member A"]
        B["member B"]
    end
    subgraph G2["group: notification-service"]
        C["member C"]
    end
    P0 & P1 --> A
    P2 & P3 --> B
    P0 & P1 & P2 & P3 --> C
```

## قاعدتين بس
| Rule | النتيجة |
|---|---|
| **داخل** group: partition → member واحد | load balancing (queue semantics) |
| **بين** groups: كل group ياخد كل شي | fan-out (pub/sub semantics) |

## Members vs Partitions (6 partitions)
| Members | Distribution | Note |
|---|---|---|
| 1 | 6 | |
| 2 | 3 / 3 | |
| 3 | 2 / 2 / 2 | |
| 4 | 2 / 2 / 1 / 1 | |
| 6 | 1 × 6 | max parallelism |
| 8 | 1 × 6 + **2 idle** | ❌ هدر |

## Group Coordinator
```mermaid
sequenceDiagram
    participant M as Member
    participant GC as Group Coordinator (a broker)
    M->>GC: FindCoordinator(group.id)
    Note over GC: broker = leader of<br/>__consumer_offsets partition<br/>hash(group.id) % 50
    M->>GC: JoinGroup
    GC-->>M: assignment
    loop every 3s
        M->>GC: Heartbeat
    end
    M->>GC: OffsetCommit(p0=120)
```

## جرّب (3 terminals)
```bash
dotnet run --project examples/dotnet/04-ConsumerGroup -- A
dotnet run --project examples/dotnet/04-ConsumerGroup -- B
dotnet run --project examples/dotnet/04-ConsumerGroup -- C
```
Output حقيقي (A أول، بعد 12s دخل B):
```text
[A] ➕ assigned 0,1,2,3,4,5   → now owns: [0,1,2,3,4,5]
[A] ➖ revoked 0,1,2          → now owns: [3,4,5]     ← 3,4,5 ما وقفوا (cooperative)
[B] ➕ assigned 2,1,0         → now owns: [0,1,2]
```

## Key Points
- group.id = هوية الـ service
- Scaling = زيد members لحد عدد partitions
- Offsets محفوظة per group

Next → [17-rebalancing.md](17-rebalancing.md)
