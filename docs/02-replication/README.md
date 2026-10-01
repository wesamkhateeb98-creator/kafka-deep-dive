# 🛡️ 02 · Replication — النسخ والموثوقية

> leader/followers، ISR، High Watermark، min.insync.replicas

```mermaid
flowchart LR
    N0["09 · Replication: Leader & Followers"]
    N1["10 · ISR, High Watermark, min.insync.replicas"]
    N0 --> N1
```

| # | Idea | One line |
|---|---|---|
| 09 | [Replication: Leader & Followers](09-replication.md) | كل partition إلها **RF** نسخ على brokers مختلفين. نسخة وحدة **Leader** (كل الكتابة والقراءة)، والباقي **Followers** بيسحبوا منه. |
| 10 | [ISR, High Watermark, min.insync.replicas](10-isr-hw.md) | **ISR** (In-Sync Replicas) = الـ replicas اللي لاحقين الـ leader. هم بس المرشحين يصيروا leader بدون فقدان داتا. |

← [Foundations](../01-foundations/README.md) · [Map](../../README.md) · [Producer](../03-producer/README.md) →
