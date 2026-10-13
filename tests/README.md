# Testing Guide

Tests are organized into four tiers under `tests/`:

- `tests/unit/` — unit tests for individual modules
- `tests/smoke/` — validation of the Docker Compose configuration
- `tests/property/` — Hypothesis property-based tests
- `tests/integration/` — end-to-end tests against a live Kafka cluster

## Install test dependencies

A virtual environment is recommended so these don't touch your system Python:

```bash
python3 -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate
pip install -r tests/requirements.txt
```

`tests/requirements.txt` includes both the test tooling and the application
libraries the unit tests import (`confluent-kafka`, `faker`, `pyspark`), so no
other install step is needed.

## Unit, smoke, and property tests (no infrastructure required)

These tiers run without any running containers:

```bash
pytest tests/unit tests/smoke tests/property
```

## Integration tests

Integration tests exercise the pipeline against a **live** Docker Compose stack
(two Kafka brokers with SASL/PLAIN auth and ACLs). They are **skip-guarded**:
rather than failing, each test is **skipped** unless all of the following hold:

1. The `confluent-kafka` client library is installed.
2. The `KAFKA_BOOTSTRAP_SERVERS` environment variable is set (this is the opt-in
   signal that a live cluster is available).
3. The configured brokers are actually reachable.

If any condition is not met, the integration tests are reported as **skipped**,
not failed.

### Why you can't just use `localhost:9092`

By design, the Kafka brokers are **internal-only**:

- The `data-net` network is declared `internal: true`.
- Neither `kafka1` nor `kafka2` publishes a port to the host.
- The brokers advertise internal listeners only
  (`KAFKA_ADVERTISED_LISTENERS: INTERNAL://kafka1:9092` / `kafka2:9092`), so
  even a published port would redirect clients to the unresolvable
  `kafka1`/`kafka2` hostnames.

As a result, the brokers are **not reachable from the host**, and pointing the
tests at `localhost:9092` will fail the reachability guard and skip. The
supported way to run the integration tier non-skipped is to execute pytest
**inside a container attached to the internal `data-net` network**, where the
`kafka1`/`kafka2` hostnames resolve. This keeps the internal-only security
posture intact (no host-exposed Kafka port).

### Running the integration tests in-network

1. **Start the stack** (if it isn't already running) and let `kafka-init`
   create the topics and ACLs:

   ```bash
   docker compose up -d
   docker compose ps          # confirm kafka1/kafka2 are healthy
   ```

2. **Confirm the internal network name.** Compose prefixes it with the project
   directory name (e.g. `realtime-data-pipeline_data-net`):

   ```bash
   docker network ls --filter name=data-net
   ```

3. **Build a throwaway runner image** with the test client dependencies baked
   in. This is necessary because `data-net` is `internal: true`, so a container
   attached to it has **no internet access** to `pip install` at run time —
   the dependencies must be installed at build time on the default (bridge)
   network. Create a temporary `Dockerfile`:

   ```dockerfile
   FROM python:3.11-slim
   RUN pip install --no-cache-dir confluent-kafka pytest
   WORKDIR /app
   ```

   ```bash
   docker build -f integration-runner.Dockerfile -t rtdp-integration-runner .
   ```

4. **Run the integration tests** from a container attached to `data-net`, with
   the project mounted and the bootstrap servers pointed at the internal broker
   hostnames:

   ```bash
   docker run --rm \
     --network realtime-data-pipeline_data-net \
     -v "$PWD":/app -w /app \
     -e KAFKA_BOOTSTRAP_SERVERS=kafka1:9092,kafka2:9092 \
     rtdp-integration-runner \
     python -m pytest tests/integration -v
   ```

   The full end-to-end test produces raw records and waits for the
   Processing_Service to close an aggregation window, so the run can take a
   couple of minutes.

5. **Clean up** the throwaway image (and the temporary Dockerfile) when done:

   ```bash
   docker rmi rtdp-integration-runner
   rm integration-runner.Dockerfile
   ```

   Stop the stack with `docker compose down` if you started it just for the
   test run.

> **Note on `pip install -r tests/requirements.txt` locally:** installing the
> test deps on the host still lets you run the unit/smoke/property tiers, but
> the integration tier will skip on the host because the brokers are not
> reachable from `localhost` (see above). Run it in-network as shown here.
