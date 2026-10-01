# 39 · Self-Test (أسئلة مقابلات)

> غطّي الجواب، جاوب بصوت عالي، بعدين افتح. كل سؤال مربوط بالـ doc.

```mermaid
flowchart LR
    A["Basics 1-10"] --> B["Producer 11-18"] --> C["Consumer 19-27"] --> D["Advanced 28-40"]
```

## Basics
<details><summary>1. ليش Kafka أسرع من queue تقليدي؟</summary>sequential I/O + page cache + zero-copy + batching/compression. [22](../06-storage/22-storage-internals.md)</details>
<details><summary>2. الفرق بين topic و partition؟</summary>topic منطقي، partition = log فيزيائي؛ وحدة الترتيب والتوازي. [06](../01-foundations/06-partitions.md)</details>
<details><summary>3. هل الترتيب مضمون بالـ topic؟</summary>لا، per partition فقط. [06](../01-foundations/06-partitions.md)</details>
<details><summary>4. شو بيصير للرسالة بعد ما تنقرأ؟</summary>ولا شي — بتبقى لحد retention/compaction. [02](../01-foundations/02-commit-log.md)</details>
<details><summary>5. شو دور الـ controller بـ KRaft؟</summary>يدير metadata (leaders, topics, ISR) عبر Raft quorum بـ `__cluster_metadata`. [04](../01-foundations/04-kraft.md)</details>
<details><summary>6. ليش 3 controllers مش 2؟</summary>Raft يحتاج majority؛ 2 ما بتتحمل ولا فشل، 3 بتتحمل 1. [04](../01-foundations/04-kraft.md)</details>
<details><summary>7. advertised.listeners لشو؟</summary>العنوان اللي الـ broker يعطيه للـ clients بالـ metadata. [03](../01-foundations/03-broker-cluster.md)</details>
<details><summary>8. الفرق بين LEO و HW؟</summary>LEO = الـ offset الجاي بالـ replica؛ HW = min(LEO) عبر الـ ISR = حد القراءة. [10](../02-replication/10-isr-hw.md)</details>
<details><summary>9. شو الـ ISR؟ ومتى replica بتطلع منه؟</summary>replicas لاحقة؛ بتطلع لو تأخرت > replica.lag.time.max.ms (30s). [10](../02-replication/10-isr-hw.md)</details>
<details><summary>10. RF=3, min.isr=2، وقع broker. بتقدر تكتب acks=all؟ ووقع التاني؟</summary>أول وقوع ✅ (ISR=2)، التاني ❌ NOT_ENOUGH_REPLICAS. [10](../02-replication/10-isr-hw.md)</details>

## Producer
<details><summary>11. acks=1 — متى بتضيع داتا؟</summary>leader ack ومات قبل ما الـ followers ينسخوا. [12](../03-producer/12-acks-durability.md)</details>
<details><summary>12. كيف idempotent producer بيمنع التكرار؟</summary>PID + sequence per partition؛ الـ broker بيرفض seq مكرر. [13](../03-producer/13-idempotent-producer.md)</details>
<details><summary>13. هل idempotence بتحمي من restart للـ app؟</summary>لا، PID جديد. بدك transactional.id. [13](../03-producer/13-idempotent-producer.md)</details>
<details><summary>14. linger.ms شو بيعمل؟</summary>ينتظر ليعبي batch → throughput أعلى، latency أعلى. [11](../03-producer/11-producer-internals.md)</details>
<details><summary>15. ليش .NET و Java ممكن يكتبوا نفس الـ key لـ partitions مختلفة؟</summary>librdkafka default = consistent_random (CRC32)، Java = murmur2. [14](../03-producer/14-partitioning-keys.md)</details>
<details><summary>16. شو بيصير لو زدت partitions؟</summary>hash % N يتغير → keys تنتقل → ترتيب الـ key ينكسر. [06](../01-foundations/06-partitions.md)</details>
<details><summary>17. hot partition — شو السبب والحل؟</summary>key skewed؛ key أدق أو salting. [14](../03-producer/14-partitioning-keys.md)</details>
<details><summary>18. dual-write problem وحله؟</summary>DB + Kafka بدون transaction مشتركة؛ Outbox + relay/CDC. [25](../07-integration-patterns/25-outbox-pattern.md)</details>

## Consumer
<details><summary>19. 6 partitions و 8 consumers بنفس الـ group؟</summary>6 شغالين، 2 idle. [16](../04-consumer/16-consumer-groups.md)</details>
<details><summary>20. كيف تعمل fan-out لنفس الـ event؟</summary>group.id مختلف لكل service. [16](../04-consumer/16-consumer-groups.md)</details>
<details><summary>21. شو بيطلق rebalance؟</summary>join/leave/death/max.poll exceeded/partitions change. [17](../04-consumer/17-rebalancing.md)</details>
<details><summary>22. eager vs cooperative؟</summary>eager يسحب كل شي؛ cooperative بس الـ partitions المنقولة. [17](../04-consumer/17-rebalancing.md)</details>
<details><summary>23. static membership لشو؟</summary>restart سريع بدون rebalance (GroupInstanceId). [17](../04-consumer/17-rebalancing.md)</details>
<details><summary>24. session.timeout vs max.poll.interval؟</summary>الأول heartbeats (process حي)، التاني poll calls (loop مش عالق). [15](../04-consumer/15-consumer-poll-loop.md)</details>
<details><summary>25. commit(offset 41) شو بيحفظ؟</summary>42 = next to read. [07](../01-foundations/07-offsets.md)</details>
<details><summary>26. وين بتنحفظ الـ offsets؟</summary>`__consumer_offsets` (compacted, 50 partitions). [18](../04-consumer/18-offset-commit.md)</details>
<details><summary>27. auto.offset.reset متى بيطبق؟</summary>لما ما في committed offset أو خارج الـ range. [07](../01-foundations/07-offsets.md)</details>

## Advanced
<details><summary>28. at-least-once vs at-most-once — بالكود؟</summary>commit بعد المعالجة vs قبلها. [19](../05-delivery-semantics/19-delivery-semantics.md)</details>
<details><summary>29. كيف تعمل exactly-once لـ Kafka→DB؟</summary>at-least-once + dedup table بنفس DB transaction. [19](../05-delivery-semantics/19-delivery-semantics.md)</details>
<details><summary>30. SendOffsetsToTransaction لشو؟</summary>يخلي commit الـ input offset جزء من نفس transaction الـ output. [20](../05-delivery-semantics/20-transactions-eos.md)</details>
<details><summary>31. zombie fencing؟</summary>epoch يزيد مع كل InitTransactions؛ القديم مرفوض. [20](../05-delivery-semantics/20-transactions-eos.md)</details>
<details><summary>32. read_committed شو بيشوف؟</summary>committed فقط، لحد LSO. [20](../05-delivery-semantics/20-transactions-eos.md)</details>
<details><summary>33. compaction شو بيضمن؟</summary>آخر value لكل key على الأقل؛ tombstone يحذف. [21](../06-storage/21-retention-compaction.md)</details>
<details><summary>34. ليش record ممكن يعيش أكثر من retention.ms؟</summary>الحذف per segment، والـ active segment ما بينحذف. [21](../06-storage/21-retention-compaction.md)</details>
<details><summary>35. poison pill — شو هو وكيف تتعامل؟</summary>record ما بتنقرأ/بتفشل دائماً؛ DLQ ولا توقف الـ partition. [24](../07-integration-patterns/24-error-handling-dlq.md)</details>
<details><summary>36. BACKWARD compatibility؟</summary>consumer الجديد يقرأ داتا قديمة؛ إضافة field مع default. [23](../07-integration-patterns/23-serialization-schema.md)</details>
<details><summary>37. كيف تحسب عدد partitions؟</summary>max(T/Tp, T/Tc) × growth. [32](../08-operations/32-partition-sizing.md)</details>
<details><summary>38. Share groups شو بتحل؟</summary>أكثر من consumer per partition + ack per record (queue semantics). [33](../08-operations/33-kafka-4x-new-features.md)</details>
<details><summary>39. أهم metric للمراقبة؟</summary>consumer lag، ثم URP و offline partitions. [29](../08-operations/29-monitoring.md)</details>
<details><summary>40. ليش ما نستخدم Kafka كـ DB؟</summary>ما في index/query by id؛ access sequential فقط. [37](37-anti-patterns.md)</details>

← Back to [README](../../README.md)
