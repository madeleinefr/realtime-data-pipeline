# Requirements Specification & Traceability

This document defines the functional (FR) and non-functional (NFR) requirements
that the source code and tests reference via `Requirements:` / `Validates:`
annotations, and maps each requirement to the code that implements it and the
tests that verify it.

Requirement groups:

- **FR-1** — Ingestion_Service (data generation and production)
- **FR-2** — Messaging / Kafka transport semantics
- **FR-3** — Processing_Service (validation, filtering, transformation, aggregation)
- **NFR-1 / NFR-2** — Architecture & deployment (microservices, IaC)
- **NFR-3** — Reliability & fault tolerance
- **NFR-5** — Security, governance & protection
- **NFR-6** — Observability & data quality
- **NFR-7** — Maintainability

---

## FR-1 — Ingestion_Service

| ID | Requirement |
|----|-------------|
| FR-1.1 | The Ingestion_Service produces JSON sensor records to the `raw_data` Kafka topic over the Kafka protocol. |
| FR-1.2 | Record generation is deterministic under a fixed random seed (the same seed reproduces an identical record sequence). |
| FR-1.3 | The production rate is configurable (records/second) via `PRODUCE_RATE`, defaulting to a safe value when unset or invalid. |
| FR-1.4 | Each generated record conforms to the defined schema with values within valid ranges (geo bounds, sensor ranges, known canton codes). |
| FR-1.5 | Records are keyed by `device_id` so all messages from a device land on the same partition (per-device ordering). |
| FR-1.6 | On connection failure the producer retries with exponential backoff. |
| FR-1.7 | Backoff is bounded; the service keeps attempting to reconnect within the bound rather than crashing immediately. |

## FR-2 — Messaging / Kafka transport

| ID | Requirement |
|----|-------------|
| FR-2.3 | Multiple consumer groups can read the same topic independently (one group's progress does not affect another's). |
| FR-2.4 | Consumers resume from their last committed offset after a restart, without redelivery of already-committed records. |
| FR-2.6 | Authorized writes are delivered at least once. |

## FR-3 — Processing_Service

| ID | Requirement |
|----|-------------|
| FR-3.1 | The Processing_Service consumes from `raw_data` via PySpark Structured Streaming. |
| FR-3.2 | Records are validated: a non-null, ISO 8601-parseable `timestamp` is required; malformed records are discarded. |
| FR-3.3 | Field transformations are applied and reflected in the output (Celsius→Fahrenheit, canton standardization, derived `pressure_category`). |
| FR-3.4 | Windowed aggregation computes count, sum, and average for numeric fields, grouped by canton. |
| FR-3.5 | Aggregation results are written to the `aggregated_data` Kafka topic. |
| FR-3.6 | Malformed or non-numeric values are handled gracefully during aggregation (skipped, not fatal). |
| FR-3.7 | Aggregation uses a configurable sliding window (`WINDOW_SIZE_MINUTES`, `SLIDE_INTERVAL_SECONDS`). |
| FR-3.8 | Late arrivals (records behind the watermark) are discarded and counted. |

## NFR-1 / NFR-2 — Architecture & deployment

| ID | Requirement |
|----|-------------|
| NFR-1.1 | Each component is defined as its own container/image (microservice isolation). |
| NFR-1.4 | Each service declares explicit resource limits (CPU/memory). |
| NFR-1.5 | Service container names / network aliases are unique. |
| NFR-2.1 | All services are defined declaratively in the Compose file (IaC). |
| NFR-2.3 | Official images are used where applicable. |
| NFR-2.4 | No credentials are hardcoded; values are supplied via environment variables. |
| NFR-2.6 | Health checks are defined for long-running services. |

## NFR-3 — Reliability & fault tolerance

| ID | Requirement |
|----|-------------|
| NFR-3.1 | Long-running services use an `on-failure` restart policy; the init container does not restart. |
| NFR-3.3 | The Processing_Service recovers streaming state from checkpoints and resumes from the last committed offset after a restart. |
| NFR-3.4 | If the ingestion producer cannot deliver for longer than a fixed timeout (60 s), it flushes and exits non-zero rather than hanging silently. |

## NFR-5 — Security, governance & protection

| ID | Requirement |
|----|-------------|
| NFR-5.1 | The internal data network is isolated (`internal: true`), blocking external access. |
| NFR-5.2 | Only the Visualization_Service exposes a host port, and only `3000`. |
| NFR-5.3 | Every Kafka connection requires SASL/PLAIN authentication. |
| NFR-5.4 | `.env` is git-ignored and only `.env.example` (placeholders) is committed. |
| NFR-5.5 | Logs are emitted as structured JSON (auditability). |
| NFR-5.6 | Invalid authentication attempts are rejected. |
| NFR-5.7 | Per-service Kafka ACLs enforce least privilege (ingestion write-only to `raw_data`; processing read `raw_data` + write `aggregated_data`; visualization read-only `aggregated_data`). |
| NFR-5.8 | Column-level exclusion filtering can remove configured fields before aggregation (data minimization). |
| NFR-5.9 | Row-level inclusion filtering admits only records matching configured criteria. |
| NFR-5.10 | Producer delivery guarantees are explicit and durable (`enable.idempotence=true`, `acks=all`). |

## NFR-6 — Observability & data quality

| ID | Requirement |
|----|-------------|
| NFR-6.2 | Each log entry carries required structured fields (timestamp, level, service, message). |
| NFR-6.3 | Images are pinned to semantic version tags (no `latest`). |
| NFR-6.6 | Filtering rejects records that are missing a required field or whose value does not match the criterion. |
| NFR-6.7 | The filter predicate is total: it always returns a boolean and never raises on valid input. |

## NFR-7 — Maintainability

| ID | Requirement |
|----|-------------|
| NFR-7.2 | Processing configuration is environment-driven and parsed defensively (invalid config falls back to safe defaults). |

---

## Traceability matrix

Each requirement maps to the primary implementation site and the test(s) that
verify it. Paths are relative to the repository root.

| ID | Implemented in | Verified by |
|----|----------------|-------------|
| FR-1.1 | `ingestion_service/producer.py`, `ingestion_service/main.py` | `tests/unit/test_producer.py` |
| FR-1.2 | `ingestion_service/generator.py` | `tests/unit/test_ingestion.py`, `tests/property/test_generator_props.py` |
| FR-1.3 | `ingestion_service/main.py` (`get_produce_rate`) | `tests/unit/test_ingestion.py` |
| FR-1.4 | `ingestion_service/generator.py` | `tests/unit/test_ingestion.py`, `tests/property/test_generator_props.py` |
| FR-1.5 | `ingestion_service/producer.py` (`produce_record`) | `tests/unit/test_producer.py` |
| FR-1.6 | `ingestion_service/retry.py` | `tests/unit/test_retry.py`, `tests/property/test_retry_props.py` |
| FR-1.7 | `ingestion_service/retry.py`, `ingestion_service/main.py` | `tests/unit/test_retry.py`, `tests/property/test_retry_props.py` |
| FR-2.3 | `kafka/create-topics.sh` (ACLs), Kafka config | `tests/integration/test_pipeline_e2e.py` |
| FR-2.4 | Spark checkpointing (`processing_service/pipeline.py`) | `tests/integration/test_failure_recovery.py`, `tests/integration/test_pipeline_e2e.py` |
| FR-2.6 | `ingestion_service/producer.py` (`acks=all`) | `tests/integration/test_kafka_security.py` |
| FR-3.1 | `processing_service/main.py`, `processing_service/pipeline.py` | `tests/integration/test_pipeline_e2e.py` |
| FR-3.2 | `processing_service/validation.py`, `processing_service/pipeline.py` | `tests/unit/test_processing.py`, `tests/property/test_processing_props.py` |
| FR-3.3 | `processing_service/pipeline.py` (transform step) | `tests/integration/test_pipeline_e2e.py` |
| FR-3.4 | `processing_service/aggregation.py`, `processing_service/pipeline.py` | `tests/unit/test_processing.py`, `tests/property/test_processing_props.py` |
| FR-3.5 | `processing_service/pipeline.py` (Kafka sink) | `tests/integration/test_pipeline_e2e.py` |
| FR-3.6 | `processing_service/aggregation.py` | `tests/unit/test_processing.py` |
| FR-3.7 | `processing_service/pipeline.py`, `processing_service/aggregation.py` | `tests/unit/test_processing.py` |
| FR-3.8 | `processing_service/aggregation.py` (late-arrival counter) | `tests/unit/test_processing.py` |
| NFR-1.1 | `docker-compose.yml` | `tests/smoke/test_compose_structure.py` |
| NFR-1.4 | `docker-compose.yml` (`deploy.resources.limits`) | `tests/smoke/test_compose_structure.py` |
| NFR-1.5 | `docker-compose.yml` (`container_name`) | `tests/smoke/test_compose_structure.py` |
| NFR-2.1 | `docker-compose.yml` | `tests/smoke/test_compose_structure.py` |
| NFR-2.3 | `docker-compose.yml` (image tags) | `tests/smoke/test_compose_structure.py` |
| NFR-2.4 | `docker-compose.yml`, `.env.example` | `tests/smoke/test_compose_structure.py` |
| NFR-2.6 | `docker-compose.yml` (`healthcheck`) | `tests/smoke/test_compose_structure.py` |
| NFR-3.1 | `docker-compose.yml` (`restart`) | `tests/smoke/test_compose_structure.py` |
| NFR-3.3 | `processing_service/pipeline.py` (checkpointing) | `tests/integration/test_failure_recovery.py` |
| NFR-3.4 | `ingestion_service/main.py` (connection-loss timeout) | `tests/unit/test_ingestion.py`, `tests/property/test_retry_props.py` |
| NFR-5.1 | `docker-compose.yml` (`data-net` internal) | `tests/smoke/test_compose_structure.py` |
| NFR-5.2 | `docker-compose.yml` (port mapping) | `tests/smoke/test_compose_structure.py` |
| NFR-5.3 | `*/producer.py`, `pipeline.py`, Kafka SASL config | `tests/integration/test_kafka_security.py` |
| NFR-5.4 | `.gitignore`, `.env.example` | `tests/smoke/test_compose_structure.py` |
| NFR-5.5 | `*/logger.py` | `tests/property/test_logging_props.py` |
| NFR-5.6 | Kafka SASL config, `kafka/kafka_server_jaas.conf` | `tests/integration/test_kafka_security.py` |
| NFR-5.7 | `kafka/create-topics.sh` (ACLs) | `tests/integration/test_kafka_security.py` |
| NFR-5.8 | `processing_service/pipeline.py` (column exclusion), `processing_service/filter.py` | `tests/unit/test_processing.py` |
| NFR-5.9 | `processing_service/filter.py`, `processing_service/pipeline.py` (row inclusion) | `tests/unit/test_processing.py`, `tests/property/test_filter_props.py`, `tests/property/test_processing_props.py` |
| NFR-5.10 | `ingestion_service/producer.py` (idempotence/acks) | `tests/unit/test_producer.py` |
| NFR-6.2 | `*/logger.py` | `tests/property/test_logging_props.py` |
| NFR-6.3 | `docker-compose.yml` (semantic image tags) | `tests/smoke/test_compose_structure.py` |
| NFR-6.6 | `processing_service/filter.py` | `tests/property/test_filter_props.py` |
| NFR-6.7 | `processing_service/filter.py` | `tests/property/test_filter_props.py` |
| NFR-7.2 | `processing_service/pipeline.py` (`PipelineConfig`) | `tests/unit/test_processing.py` |
