"""PySpark Structured Streaming pipeline for the processing service.

Implements the full streaming pipeline that reads from the raw_data Kafka topic,
deserializes JSON records, validates timestamps, applies row-level and column-level
filtering, computes sliding window aggregations grouped by canton, and writes
results to the aggregated_data Kafka topic with checkpointing and retry logic.

Configuration is driven entirely by environment variables. SASL/PLAIN authentication is 
configured for both Kafka consumers and producers.

Requirements: FR-3.1, FR-3.2, FR-3.3, FR-3.4, FR-3.5, FR-3.6, FR-3.7, FR-3.8,
              NFR-3.3, NFR-5.5, NFR-5.8, NFR-5.9, NFR-5.10, NFR-7.2
"""

import json
import os
import time
import threading
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
)

from processing_service.logger import get_logger

logger = get_logger("pipeline")

# Thread-safe cumulative counter for total records ingested across all cantons
_total_records_lock = threading.Lock()
_total_records_ingested: int = 0


# --- Schema Definition ---

RAW_DATA_SCHEMA = StructType([
    StructField("record_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("device_id", StringType(), True),
    StructField("device_type", StringType(), True),
    StructField("canton", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True),
    StructField("pressure", DoubleType(), True),
    StructField("status", StringType(), True),
])


# --- Configuration ---


class PipelineConfig:
    """Configuration container for the streaming pipeline.

    Reads all configuration from environment variables with sensible defaults
    matching the design document.

    Attributes:
        bootstrap_servers: Kafka broker addresses.
        input_topic: Source Kafka topic to consume from.
        output_topic: Sink Kafka topic to produce to.
        window_size_minutes: Aggregation window size in minutes.
        slide_interval_seconds: Window slide interval in seconds.
        column_exclusion_list: List of column names to exclude from processing.
        row_inclusion_criteria: Dictionary of field-to-value inclusion filters.
        checkpoint_dir: Spark checkpoint directory path.
        sasl_username: SASL/PLAIN username for Kafka authentication.
        sasl_password: SASL/PLAIN password for Kafka authentication.
        max_write_retries: Maximum number of write retries.
        retry_base_seconds: Base interval for exponential backoff in seconds.
    """

    def __init__(self) -> None:
        """Initialize pipeline configuration from environment variables."""
        self.bootstrap_servers: str = os.environ.get(
            "KAFKA_BOOTSTRAP_SERVERS", "kafka1:9092,kafka2:9092"
        )
        self.input_topic: str = os.environ.get("INPUT_TOPIC", "raw_data")
        self.output_topic: str = os.environ.get("OUTPUT_TOPIC", "aggregated_data")
        self.window_size_minutes: int = int(
            os.environ.get("WINDOW_SIZE_MINUTES", "5")
        )
        self.slide_interval_seconds: int = int(
            os.environ.get("SLIDE_INTERVAL_SECONDS", "60")
        )
        self.checkpoint_dir: str = os.environ.get(
            "CHECKPOINT_DIR", "/tmp/spark-checkpoints"
        )
        self.sasl_username: str = os.environ.get("KAFKA_SASL_USERNAME", "")
        self.sasl_password: str = os.environ.get("KAFKA_SASL_PASSWORD", "")
        self.max_write_retries: int = 3
        self.retry_base_seconds: float = 1.0

        # Parse column exclusion list
        raw_exclusion = os.environ.get("COLUMN_EXCLUSION_LIST", "")
        self.column_exclusion_list: List[str] = [
            col.strip()
            for col in raw_exclusion.split(",")
            if col.strip()
        ]

        # Parse row inclusion criteria
        raw_criteria = os.environ.get("ROW_INCLUSION_CRITERIA", "{}")
        try:
            self.row_inclusion_criteria: Dict[str, Any] = json.loads(
                raw_criteria if raw_criteria.strip() else "{}"
            )
        except json.JSONDecodeError:
            logger.error(
                f"Invalid ROW_INCLUSION_CRITERIA JSON: {raw_criteria}, "
                f"defaulting to empty (no row filtering)"
            )
            self.row_inclusion_criteria = {}


# --- Filter Builders ---


def build_inclusion_filter(
    inclusion_criteria: Dict[str, Any],
) -> Optional[str]:
    """Build a Spark SQL filter expression from inclusion criteria.

    Constructs an AND-combined filter expression where each criterion
    requires the field value to equal the specified value (single value)
    or be contained in a list of acceptable values.

    Args:
        inclusion_criteria: A dictionary mapping field names to acceptable
            values. Each value can be a single value or a list of values.

    Returns:
        A Spark SQL expression string for use with DataFrame.filter(),
        or None if no criteria are specified.
    """
    if not inclusion_criteria:
        return None

    conditions: List[str] = []
    for field, criterion in inclusion_criteria.items():
        if isinstance(criterion, list):
            # Build IN clause
            values_str = ", ".join(
                f"'{v}'" if isinstance(v, str) else str(v)
                for v in criterion
            )
            conditions.append(f"`{field}` IN ({values_str})")
        else:
            if isinstance(criterion, str):
                conditions.append(f"`{field}` = '{criterion}'")
            else:
                conditions.append(f"`{field}` = {criterion}")

    return " AND ".join(conditions)


# --- Retry Logic ---


def write_batch_with_retry(
    batch_df: DataFrame,
    config: PipelineConfig,
    kafka_options: Dict[str, str],
) -> None:
    """Write a micro-batch DataFrame to Kafka with exponential backoff retry.

    Attempts to write the batch up to max_write_retries times. On failure,
    waits with exponential backoff (base * 2^attempt) before retrying.

    Args:
        batch_df: The Spark DataFrame representing the micro-batch to write.
        config: Pipeline configuration containing retry parameters.
        kafka_options: Dictionary of Kafka writer options.

    Raises:
        Exception: Re-raises the last exception if all retries are exhausted.
    """
    last_exception: Optional[Exception] = None

    for attempt in range(config.max_write_retries):
        try:
            (
                batch_df.write
                .format("kafka")
                .options(**kafka_options)
                .save()
            )
            return
        except Exception as e:
            last_exception = e
            if attempt < config.max_write_retries - 1:
                wait_time = config.retry_base_seconds * (2 ** attempt)
                logger.error(
                    f"Write attempt {attempt + 1}/{config.max_write_retries} "
                    f"failed: {e}. Retrying in {wait_time:.1f}s"
                )
                time.sleep(wait_time)
            else:
                logger.error(
                    f"Write attempt {attempt + 1}/{config.max_write_retries} "
                    f"failed: {e}. All retries exhausted."
                )

    if last_exception is not None:
        raise last_exception


# --- Batch Processing (foreachBatch) ---


def create_foreach_batch_handler(
    config: PipelineConfig,
    kafka_options: Dict[str, str],
):
    """Create a foreachBatch handler function for the streaming query.

    The returned function processes each micro-batch by formatting
    aggregation results as JSON and writing them to the output Kafka topic
    with retry logic.

    Args:
        config: Pipeline configuration.
        kafka_options: Dictionary of Kafka writer options.

    Returns:
        A callable suitable for use with writeStream.foreachBatch().
    """
    def process_batch(batch_df: DataFrame, batch_id: int) -> None:
        """Process a single micro-batch of aggregated data.

        Formats the aggregation results into the output JSON schema and
        writes to Kafka with retry logic. Logs per-batch statistics.

        Args:
            batch_df: The micro-batch DataFrame containing aggregation results.
            batch_id: The unique identifier for this micro-batch.
        """
        global _total_records_ingested

        if batch_df.rdd.isEmpty():
            return

        # Count records in this batch for logging
        batch_count = batch_df.count()

        # Update cumulative total records counter
        # Sum record_count across all rows in this batch
        total_in_batch = batch_df.agg(
            F.sum("record_count").alias("total")
        ).collect()[0]["total"] or 0

        with _total_records_lock:
            _total_records_ingested += int(total_in_batch)
            current_total = _total_records_ingested

        # Add computed_at and total_records_ingested columns
        computed_at = datetime.now(timezone.utc).isoformat()

        output_df = batch_df.select(
            F.date_format(
                F.col("window.start"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
            ).alias("window_start"),
            F.date_format(
                F.col("window.end"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
            ).alias("window_end"),
            F.col("canton"),
            F.col("record_count"),
            F.col("device_count"),
            F.lit(current_total).alias("total_records_ingested"),
            F.col("avg_temperature"),
            F.col("avg_humidity"),
            F.col("avg_pressure"),
            F.lit(computed_at).alias("computed_at"),
        )

        # Serialize to JSON and prepare for Kafka (key=canton, value=JSON)
        kafka_df = output_df.select(
            F.col("canton").cast(StringType()).alias("key"),
            F.to_json(F.struct("*")).alias("value"),
        )

        # Write with retry
        write_batch_with_retry(kafka_df, config, kafka_options)

        # Log batch statistics in structured JSON format
        log_entry = json.dumps({
            "event": "batch_processed",
            "batch_id": batch_id,
            "records_processed": int(total_in_batch),
            "records_discarded": 0,
            "records_aggregated": batch_count,
        })
        logger.info(log_entry)

    return process_batch


# --- Main Pipeline ---


def build_kafka_options(config: PipelineConfig, is_reader: bool = True) -> Dict[str, str]:
    """Build Kafka connection options with SASL/PLAIN authentication.

    Constructs the option dictionary for Spark's Kafka connector,
    including bootstrap servers and SASL authentication configuration.

    Args:
        config: Pipeline configuration containing Kafka connection details.
        is_reader: If True, includes subscribe option for the input topic.
            If False, includes topic option for the output topic.

    Returns:
        A dictionary of Kafka options for use with Spark DataFrameReader
        or DataFrameWriter.
    """
    options: Dict[str, str] = {
        "kafka.bootstrap.servers": config.bootstrap_servers,
    }

    if config.sasl_username and config.sasl_password:
        options["kafka.security.protocol"] = "SASL_PLAINTEXT"
        options["kafka.sasl.mechanism"] = "PLAIN"
        jaas_config = (
            "org.apache.kafka.common.security.plain.PlainLoginModule required "
            f'username="{config.sasl_username}" '
            f'password="{config.sasl_password}";'
        )
        options["kafka.sasl.jaas.config"] = jaas_config

    if is_reader:
        options["subscribe"] = config.input_topic
        options["startingOffsets"] = "latest"
        options["failOnDataLoss"] = "false"
    else:
        options["topic"] = config.output_topic

    return options


def start_pipeline(spark: SparkSession, config: Optional[PipelineConfig] = None):
    """Build and start the PySpark Structured Streaming pipeline.

    Constructs the full streaming pipeline: reads from the input Kafka topic,
    deserializes JSON, validates timestamps, applies row-level and column-level
    filtering, computes sliding window aggregations grouped by canton, and writes
    to the output Kafka topic using foreachBatch with retry logic.

    Args:
        spark: An initialized SparkSession instance.
        config: Optional pipeline configuration. If None, a new PipelineConfig
            is created from environment variables.

    Returns:
        The StreamingQuery object representing the running pipeline.
    """
    if config is None:
        config = PipelineConfig()

    logger.info(
        f"Starting pipeline: input={config.input_topic}, "
        f"output={config.output_topic}, "
        f"window={config.window_size_minutes}min, "
        f"slide={config.slide_interval_seconds}s, "
        f"exclusions={config.column_exclusion_list}, "
        f"inclusion_criteria={config.row_inclusion_criteria}"
    )

    # Build Kafka reader options
    reader_options = build_kafka_options(config, is_reader=True)

    # 1. Read from Kafka
    raw_stream = (
        spark.readStream
        .format("kafka")
        .options(**reader_options)
        .load()
    )

    # 2. Deserialize JSON from Kafka value
    parsed = raw_stream.select(
        F.from_json(
            F.col("value").cast(StringType()),
            RAW_DATA_SCHEMA,
        ).alias("data")
    ).select("data.*")

    # 3. Validate: non-null timestamp parseable as ISO 8601
    validated = parsed.filter(
        F.col("timestamp").isNotNull()
        & (F.to_timestamp(F.col("timestamp")).isNotNull())
    )

    # 4. Row-level filtering (inclusion criteria)
    inclusion_expr = build_inclusion_filter(config.row_inclusion_criteria)
    if inclusion_expr:
        filtered = validated.filter(inclusion_expr)
        logger.info(f"Row inclusion filter applied: {inclusion_expr}")
    else:
        filtered = validated

    # 5. Column-level filtering (exclusion list)
    if config.column_exclusion_list:
        # Only drop columns that exist in the schema
        existing_cols = set(filtered.columns)
        cols_to_drop = [
            c for c in config.column_exclusion_list if c in existing_cols
        ]
        projected = filtered.drop(*cols_to_drop)
        logger.info(f"Column exclusion applied: {cols_to_drop}")
    else:
        projected = filtered

    # 6. Cast timestamp to proper TimestampType for windowing
    with_ts = projected.withColumn(
        "event_time", F.to_timestamp(F.col("timestamp"))
    )

    # 7. Apply watermark for late arrivals (30 seconds)
    watermarked = with_ts.withWatermark("event_time", "30 seconds")

    # 8. Sliding window aggregation grouped by canton
    window_duration = f"{config.window_size_minutes} minutes"
    slide_duration = f"{config.slide_interval_seconds} seconds"

    aggregated = (
        watermarked
        .groupBy(
            F.window(F.col("event_time"), window_duration, slide_duration),
            F.col("canton"),
        )
        .agg(
            F.count("*").alias("record_count"),
            F.approx_count_distinct("device_id").alias("device_count"),
            F.avg("temperature").alias("avg_temperature"),
            F.avg("humidity").alias("avg_humidity"),
            F.avg("pressure").alias("avg_pressure"),
        )
    )

    # 9. Write to Kafka using foreachBatch with retry logic
    writer_options = build_kafka_options(config, is_reader=False)
    batch_handler = create_foreach_batch_handler(config, writer_options)

    query = (
        aggregated.writeStream
        .outputMode("update")
        .foreachBatch(batch_handler)
        .option("checkpointLocation", config.checkpoint_dir)
        .start()
    )

    logger.info("Streaming pipeline started successfully")
    return query
