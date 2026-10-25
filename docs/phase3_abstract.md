# Real-Time Data Backend for IoT Sensor Telemetry

## 1. Solution Overview

This project implements a reliable, scalable, and maintainable real-time streaming
backend that ingests, processes, and visualizes continuous IoT weather telemetry.
The workload simulates weather stations across the 26 Swiss cantons reporting
temperature, humidity, and atmospheric pressure. The system is decomposed into
four logical microservices across six Docker containers orchestrated with Docker
Compose:

- **Ingestion Service** — a Python producer generating synthetic sensor payloads
  with realistic per-canton distributions and streaming them into Kafka.
- **Message Broker Tier** — a two-node Apache Kafka cluster (KRaft mode) acting as
  the decoupled streaming transport and the intermediate message store.
- **Processing Engine** — a PySpark Structured Streaming service that validates,
  filters, transforms, and aggregates the event stream.
- **Visualization Layer** — an official Grafana container with the Kafka data
  source plugin rendering live dashboards.

Data flows unidirectionally: Ingestion → raw_data topic → Processing →
aggregated_data topic → Visualization. Using Kafka as both transport and
intermediate store reduces operational overhead and removes traditional relational
database bottlenecks.

## 2. Technical Approach ("Making Of")

The system is codified entirely as Infrastructure as Code (IaC) for deterministic
reproducibility. An ephemeral initialization container sequences startup by waiting
on broker health checks, then creates the topics (raw_data, aggregated_data; 3
partitions, replication factor 2) and configures SASL/PLAIN Access Control Lists
(ACLs) to enforce least-privilege access per service.

The PySpark pipeline runs three processing stages: **validation** enforces schema
conformity and ISO 8601 timestamps, dropping malformed payloads; **filtering and
transformation** applies row- and column-level pruning, converts Celsius to
Fahrenheit, and buckets pressure into low/normal/high bands; **windowed
aggregation** computes per-canton metrics over a 5-minute sliding window with a
60-second slide, backed by disk checkpointing for fault-tolerant state recovery.
Correctness was validated locally through end-to-end integration tests, schema and
ACL boundary tests, and container health probes.

## 3. Reflection and Critical Evaluation

**Fulfillment of requirements.** The backend continuously ingests an unbounded
stream of ISO 8601-timestamped events at a configurable rate. The required volume
of at least 1,000,000 time-referenced data points is both reachable in sustained
operation and independently reproducible: a bounded verification run of the real
generator produced 1,000,000 unique records across all 26 cantons, every record
timestamped and monotonically ordered, confirming the data requirement without a
multi-hour live run. The system also guarantees durable, isolated microservice
execution, performs stateful windowed aggregation, and surfaces real-time metrics
on live dashboards.

**Challenges and root causes.** Three issues stood out. (1) Fully random data
generation produced chaotic dashboard metrics; this was resolved with a stateful
generator modeling a fixed 500-device fleet with stable per-canton baselines.
(2) The Grafana Kafka plugin is a live-tail reader with no historical querying, so
all aggregation was shifted into Spark's stateful streaming engine. (3) Combining
SASL/PLAIN authentication with fine-grained ACLs in KRaft mode required precise
sequencing during broker initialization.

**System qualities.** *Reliability*: two brokers with replication factor 2 prevent
data loss on single-broker failure, Spark checkpointing recovers state across
restarts, and the producer uses exponential connection backoff. *Scalability*:
topics are partitioned (N=3), enabling horizontal consumer scaling and buffering
upstream burst traffic. *Maintainability*: single-responsibility services,
centralized environment-based configuration, structured JSON logging, and automated
Compose deployment.

**Security and governance enhancements.** Current controls include isolated Docker
networks, authenticated SASL/PLAIN endpoints, topic-level ACLs, and payload
validation. Enterprise next steps would add TLS in-transit encryption, a centralized
secrets manager (e.g., HashiCorp Vault) in place of local .env files, a schema
registry (Confluent/Apicurio), and a third broker to enforce min.insync.replicas=2.

**Skill development.** Key technical competencies: (1) stateful stream processing
with PySpark (watermarking, windowing, checkpoints); (2) configuring and securing
distributed Kafka clusters; (3) production-grade IaC with Docker Compose.
Methodological and soft skills: (1) decomposing distributed requirements into
modular boundaries; (2) systematic root-cause troubleshooting across asynchronous
tiers; (3) clear technical documentation. For the next project I would improve my
workflow by pinning dependencies and adding continuous integration from the outset,
rather than hardening reproducibility late in the process.

## 4. Architectural Extension: Batch Processing Pipeline

To support downstream analytical and machine learning workloads, the architecture
can be extended into a canonical **Lambda Architecture**. Kafka remains the unified
streaming entry point. Alongside the PySpark real-time consumer, a batch pipeline
would be introduced using an orchestrator such as Apache Airflow. On a scheduled
cadence (e.g., daily or quarterly), a batch Spark job would read accumulated records
from the raw topic or an object-storage sink (e.g., MinIO/S3 via a Kafka Connect S3
sink connector), compute comprehensive historical aggregations, perform feature
extraction, and persist model-ready datasets into a data warehouse or Delta Lake
table. This hybrid approach preserves sub-second dashboard latency on the hot path
while providing cold-path storage for compute-intensive model retraining.
