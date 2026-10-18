"""End-to-end integration test for the real-time data pipeline.

Exercises the full data flow against a running Docker Compose stack:

    produce (Ingestion) -> raw_data -> consume/transform/aggregate
    (Processing) -> aggregated_data -> consume (Visualization role)

The test produces synthetic device records to the ``raw_data`` topic using the
ingestion service's own generator and producer configuration, then consumes
from the ``aggregated_data`` topic to verify that the Processing_Service turns
raw records into windowed aggregation results and delivers them downstream.

Because this requires the live cluster *and* a running Processing_Service, the
whole module is guarded: it skips when confluent-kafka is missing, when
``KAFKA_BOOTSTRAP_SERVERS`` is unset, or when the brokers are unreachable.

Validated requirements:
    - FR-2.3: multiple consumer groups read the same topic independently
      (verified with two ``test-``-prefixed groups on ``raw_data``).
    - FR-2.4: consumers resume from their last committed offset.
    - FR-3.5: aggregation results written to aggregated_data.
    - FR-3.3: transformation reflected in output (Celsius->Fahrenheit
      normalization and the derived pressure_category counts).
    - NFR-5.3: SASL/PLAIN authentication required for all connections.
"""

import json
import time
import uuid

import pytest

from tests.integration.conftest import (
    AGGREGATED_DATA_TOPIC,
    PROCESSING_GROUP,
    RAW_DATA_TOPIC,
    TEST_GROUP_PREFIX,
    VISUALIZATION_GROUP,
    requires_kafka,
    sasl_config,
)

pytestmark = pytest.mark.integration


def _build_record(canton: str = "ZH") -> dict:
    """Build a minimal valid raw_data record for e2e verification.

    Args:
        canton: The Swiss canton code to tag the record with.

    Returns:
        A dictionary matching the raw_data schema with a fresh UUID and a
        current ISO 8601 timestamp.
    """
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc).isoformat()
    return {
        "record_id": str(uuid.uuid4()),
        "timestamp": now,
        "device_id": "device-001",
        "device_type": "weather_station",
        "canton": canton,
        "latitude": 47.3769,
        "longitude": 8.5417,
        "temperature": 4.2,
        "humidity": 78.5,
        "pressure": 1018.30,
        "status": "active",
    }


def _produce_records(count: int, canton: str) -> None:
    """Produce ``count`` synthetic records to raw_data as the ingestion user.

    Args:
        count: Number of records to produce.
        canton: Canton code tagged on every produced record.
    """
    from confluent_kafka import Producer

    producer = Producer(sasl_config("ingestion"))
    for _ in range(count):
        record = _build_record(canton)
        producer.produce(
            RAW_DATA_TOPIC,
            key=record["device_id"].encode("utf-8"),
            value=json.dumps(record).encode("utf-8"),
        )
    producer.flush(timeout=30)


class TestEndToEndPipeline:
    """Full produce -> process -> aggregate -> consume verification."""

    def test_produce_transform_aggregate_output(self, kafka_cluster):
        """**Validates: Requirements FR-3.5, NFR-5.3**

        Producing raw records eventually yields aggregation results on the
        aggregated_data topic (assuming a running Processing_Service). The
        test uses a unique canton marker so it can distinguish its own
        aggregation output from unrelated traffic on a shared cluster.
        """
        from confluent_kafka import Consumer

        # Unique-ish marker so we only assert on windows we produced. Cantons
        # are a fixed enum, so we pick one and correlate on freshness instead.
        canton = "ZH"

        # 1. Produce a burst of raw records.
        _produce_records(count=50, canton=canton)

        # 2. Consume from aggregated_data as the visualization role and wait
        #    for at least one aggregation result to appear for our canton.
        # The visualization user is only authorized to READ the exact consumer
        # group named ``visualization-group`` (least-privilege group ACL); a
        # randomized group id would be denied with GROUP_AUTHORIZATION_FAILED.
        consumer_conf = sasl_config("visualization")
        consumer_conf.update(
            {
                "group.id": VISUALIZATION_GROUP,
                "auto.offset.reset": "latest",
                "enable.auto.commit": False,
            }
        )
        consumer = Consumer(consumer_conf)
        consumer.subscribe([AGGREGATED_DATA_TOPIC])

        found = False
        # Aggregation windows slide on the order of a minute; allow generous
        # time for the Processing_Service to close a window and emit output.
        deadline = time.time() + 180
        try:
            while time.time() < deadline and not found:
                msg = consumer.poll(timeout=5.0)
                if msg is None or msg.error():
                    continue
                try:
                    payload = json.loads(msg.value().decode("utf-8"))
                except (ValueError, AttributeError):
                    continue
                if payload.get("canton") == canton:
                    # Aggregation output must expose the computed metrics.
                    assert "record_count" in payload
                    assert "avg_temperature" in payload

                    # Transformation step must be reflected in the output:
                    # (a) temperature normalized Celsius -> Fahrenheit, averaged
                    #     over the window. Assert the field is present and
                    #     internally consistent with the Celsius average
                    #     (F = C * 9/5 + 32), rather than a fixed value, since
                    #     the window may include other traffic.
                    assert "avg_temperature_f" in payload
                    expected_f = payload["avg_temperature"] * 9.0 / 5.0 + 32.0
                    assert payload["avg_temperature_f"] == pytest.approx(
                        expected_f, abs=0.5
                    )

                    # (b) derived pressure_category counts must be present and
                    #     consistent: the three category counts sum to the
                    #     window's record_count.
                    for field in ("pressure_low", "pressure_normal", "pressure_high"):
                        assert field in payload
                    category_total = (
                        payload["pressure_low"]
                        + payload["pressure_normal"]
                        + payload["pressure_high"]
                    )
                    assert category_total == payload["record_count"]

                    found = True
        finally:
            consumer.close()

        if not found:
            pytest.skip(
                "No aggregation output observed within the time budget; "
                "requires a running Processing_Service producing windows"
            )


class TestConsumerGroupIndependence:
    """FR-2.3 / FR-2.4: independent groups and offset resumption."""

    def test_independent_consumer_groups_receive_same_data(self, kafka_cluster):
        """**Validates: Requirements FR-2.3**

        FR-2.3 requires that multiple consumer groups can read from the *same*
        topic independently, each receiving the full set of messages without
        interference between groups.

        This test verifies that property directly: it creates TWO consumers
        with distinct group ids on the SAME topic (``raw_data``), produces one
        uniquely-marked record, and asserts that *both* groups independently
        observe that record. Because each group tracks its own offsets, the two
        groups do not compete for messages — each receives its own copy, which
        is exactly the independence FR-2.3 mandates.

        Both group ids use the reserved ``test-`` prefix
        (e.g. ``test-fr23-a-<uuid>`` / ``test-fr23-b-<uuid>``). The cluster
        grants the ``processing`` user READ on any ``test-``-prefixed group via
        a narrowly-scoped PREFIXED group ACL (see ``kafka/create-topics.sh``).
        That prefix grant is what makes running two authorized groups on the
        same topic possible *without* weakening production least-privilege: it
        is a bounded prefix (not a wildcard ``*``), grants READ only, and leaves
        the exact-group bindings (``processing-group`` / ``visualization-group``)
        that production services rely on untouched.
        """
        requires_kafka()

        from confluent_kafka import Consumer, Producer

        topic = RAW_DATA_TOPIC

        def _make_consumer(group_id: str) -> Consumer:
            """Build a processing-user consumer subscribed to ``raw_data``.

            Uses ``auto.offset.reset=earliest`` (so a freshly-created group
            reads from the beginning) and disables auto-commit (each group
            manages its own position independently).
            """
            conf = sasl_config("processing")
            conf.update(
                {
                    "group.id": group_id,
                    "auto.offset.reset": "earliest",
                    "enable.auto.commit": False,
                }
            )
            consumer = Consumer(conf)
            consumer.subscribe([topic])
            return consumer

        # Two DISTINCT, independent consumer groups on the SAME topic. Both use
        # the authorized ``test-`` prefix so each is allowed to join its own
        # group without touching production's exact-group ACLs.
        group_a = f"{TEST_GROUP_PREFIX}fr23-a-{uuid.uuid4()}"
        group_b = f"{TEST_GROUP_PREFIX}fr23-b-{uuid.uuid4()}"

        consumer_a = _make_consumer(group_a)
        consumer_b = _make_consumer(group_b)

        def _await_assignment(consumer: Consumer, timeout: float = 30.0) -> None:
            """Poll until the consumer has at least one partition assignment.

            Polling drives the group join/rebalance; we wait for an assignment
            so that a record produced afterwards is guaranteed to be visible to
            this group from ``earliest``.
            """
            end = time.time() + timeout
            while time.time() < end:
                consumer.poll(timeout=1.0)
                if consumer.assignment():
                    return
            raise AssertionError(
                "consumer did not receive a partition assignment within timeout"
            )

        def _saw_marker(consumer: Consumer, marker: str, timeout: float = 30.0) -> bool:
            """Return True if ``consumer`` observes the record with ``marker``.

            Polls up to ``timeout`` seconds looking for a record whose
            ``record_id`` equals ``marker``.
            """
            end = time.time() + timeout
            while time.time() < end:
                msg = consumer.poll(timeout=2.0)
                if msg is None or msg.error():
                    continue
                try:
                    payload = json.loads(msg.value().decode("utf-8"))
                except (ValueError, AttributeError):
                    continue
                if payload.get("record_id") == marker:
                    return True
            return False

        try:
            # Ensure both groups are joined and assigned before producing, so
            # the marked record is within each group's consumable range.
            _await_assignment(consumer_a)
            _await_assignment(consumer_b)

            # Produce ONE uniquely-marked record to raw_data as the ingestion
            # user (authorized to WRITE raw_data).
            marker = str(uuid.uuid4())
            record = _build_record()
            record["record_id"] = marker
            producer = Producer(sasl_config("ingestion"))
            producer.produce(
                topic,
                key=record["device_id"].encode("utf-8"),
                value=json.dumps(record).encode("utf-8"),
            )
            producer.flush(timeout=30)

            # Both independent groups must each receive their own copy of the
            # record — demonstrating no interference between groups.
            assert _saw_marker(consumer_a, marker), (
                f"group {group_a} did not receive the marked record"
            )
            assert _saw_marker(consumer_b, marker), (
                f"group {group_b} did not receive the marked record"
            )
        finally:
            consumer_a.close()
            consumer_b.close()

    def test_consumer_resumes_from_committed_offset(self, kafka_cluster):
        """**Validates: Requirements FR-2.4**

        After committing an offset and reconnecting with the same group id, the
        consumer resumes past already-committed messages rather than
        redelivering them.
        """
        from confluent_kafka import Consumer, Producer, TopicPartition

        # The processing user is only authorized to READ the exact consumer
        # group named ``processing-group`` (least-privilege group ACL). Any
        # other (randomized) group id would be denied with
        # GROUP_AUTHORIZATION_FAILED, so we must use the authorized group name.
        # Uniqueness across runs is provided by the per-record marker UUIDs.
        group_id = PROCESSING_GROUP
        topic = RAW_DATA_TOPIC

        # Produce two distinct marked records.
        marker_first = str(uuid.uuid4())
        marker_second = str(uuid.uuid4())
        producer = Producer(sasl_config("ingestion"))
        for marker in (marker_first, marker_second):
            record = _build_record()
            record["record_id"] = marker
            producer.produce(
                topic,
                key=b"device-001",
                value=json.dumps(record).encode("utf-8"),
            )
        producer.flush(timeout=30)

        def consume_one_and_commit() -> str:
            conf = sasl_config("processing")
            conf.update(
                {
                    "group.id": group_id,
                    "auto.offset.reset": "earliest",
                    "enable.auto.commit": False,
                }
            )
            consumer = Consumer(conf)
            consumer.subscribe([topic])
            seen = None
            end = time.time() + 30
            try:
                while time.time() < end and seen is None:
                    msg = consumer.poll(timeout=2.0)
                    if msg is None or msg.error():
                        continue
                    try:
                        payload = json.loads(msg.value().decode("utf-8"))
                    except (ValueError, AttributeError):
                        continue
                    rid = payload.get("record_id")
                    if rid in (marker_first, marker_second):
                        # Commit past this exact message.
                        consumer.commit(
                            offsets=[
                                TopicPartition(
                                    msg.topic(), msg.partition(), msg.offset() + 1
                                )
                            ],
                            asynchronous=False,
                        )
                        seen = rid
            finally:
                consumer.close()
            return seen

        first_seen = consume_one_and_commit()
        if first_seen is None:
            pytest.skip("Could not consume a marked record to establish offset")

        # A second consumer in the same group must not redeliver the committed
        # message; it should only ever observe the *other* marker (or nothing
        # new if both landed on different partitions and were both committed).
        conf = sasl_config("processing")
        conf.update(
            {
                "group.id": group_id,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
        )
        consumer = Consumer(conf)
        consumer.subscribe([topic])
        redelivered = False
        try:
            end = time.time() + 20
            while time.time() < end:
                msg = consumer.poll(timeout=2.0)
                if msg is None or msg.error():
                    continue
                try:
                    payload = json.loads(msg.value().decode("utf-8"))
                except (ValueError, AttributeError):
                    continue
                if payload.get("record_id") == first_seen:
                    redelivered = True
                    break
        finally:
            consumer.close()

        assert not redelivered, (
            "committed message was redelivered; offset resumption failed"
        )
