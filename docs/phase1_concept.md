# Phase 1 Concept: Real-Time Data Backend

The architecture uses four Docker Compose microservices. The Ingestion Service (Python, Faker) continuously streams 1M+ synthetic IoT sensor records to Kafka. The Message Broker (apache/kafka:3.7.0, KRaft mode) provides durable transport with a replication factor 2. The Processing Service (custom PySpark image) applies row/column filtering and computes sliding-window aggregations (count/avg/sum by canton; 5-min window, 60-sec slide) with Spark checkpointing for fault tolerance. The Visualization Service (grafana/grafana-oss:10.4.2) acts as the optional add-on frontend delivery mechanism, rendering live dashboards directly from Kafka.

**System Qualities**

Reliability, scalability, maintainability are ensured via Docker restart policies, Kafka topic replication, 3+ partitions for horizontal consumer scaling, Spark checkpointing, JSON logging, and reproducible IaC. Security and governance utilize internal network isolation, SASL/PLAIN authentication, strict Kafka ACLs, data filtering and audit logging.

**Advantages:** The streaming-native, fully reproducible IaC-deployed design is horizontally scalable and eliminates database dependencies. Retaining data in Kafka gracefully buffers traffic spikes from producers, decoupling ingestion from processing. This reduces immediate reliance on compute platform availability and introduces a robust, layered security model.

**Disadvantages:** Local host storage limits Kafka's retention capacity. Furthermore, Kafka and Spark's complexity creates additional configuration overhead for non-experienced users, and the absence of a persistent query store prevents ad-hoc historical analysis.
