kafka is distributed, durable, partioned and append only log event.
- Distributed : Data and workloads can be spread across mutiple brokers.
- Durable : Records can be persisted and replicated to survive failures.
- Partitioned : Topic are divided into independent order logs
- Append only : New records are appended rather than modifying earlier logs 
- Event log : Consumers track their own positions through the records.

Producer : Your Node.js application that writes records to Kafka. It decides the topic, message key, and optionally partition.

Topic : A named stream of records, such as orders.created.v1, payments.completed.v1, or video.uploaded.v1.

Partition : An independently ordered log within a topic. Partitioning allows Kafka to distribute storage and processing.

Broker : A Kafka server that stores partitions and serves producers and consumers. A production cluster typically contains multiple brokers.

Consumer : An application reading records from topics and acting on them.

Consumer group : Consumers sharing a group ID coordinate the work. A partition is normally assigned to one active consumer in a conventional group at a time.

Offset : A record's position within a partition. Consumers commit the next offset they should resume from after a restart.

