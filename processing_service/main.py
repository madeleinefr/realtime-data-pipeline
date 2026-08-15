"""Entry point for the processing service.

Initializes a PySpark SparkSession configured for Structured Streaming,
starts the streaming pipeline that reads from the raw_data Kafka topic,
and awaits termination. The pipeline processes, aggregates, and writes
results to the aggregated_data Kafka topic.

Configuration via environment variables:
    - KAFKA_BOOTSTRAP_SERVERS: Broker addresses (default: kafka1:9092,kafka2:9092)
    - INPUT_TOPIC: Source topic (default: raw_data)
    - OUTPUT_TOPIC: Sink topic (default: aggregated_data)
    - WINDOW_SIZE_MINUTES: Aggregation window size (default: 5)
    - SLIDE_INTERVAL_SECONDS: Window slide interval (default: 60)
    - COLUMN_EXCLUSION_LIST: Comma-separated fields to strip (default: "")
    - ROW_INCLUSION_CRITERIA: JSON filter criteria (default: {})
    - CHECKPOINT_DIR: Spark checkpoint directory (default: /tmp/spark-checkpoints)
    - KAFKA_SASL_USERNAME: SASL username for Kafka authentication
    - KAFKA_SASL_PASSWORD: SASL password for Kafka authentication
    - LOG_LEVEL: Logging verbosity (default: INFO)

Requirements: FR-3.1, FR-3.5, FR-3.6, NFR-3.3
"""

import sys

from pyspark.sql import SparkSession

from processing_service.logger import get_logger
from processing_service.pipeline import PipelineConfig, start_pipeline

logger = get_logger("main")


def create_spark_session() -> SparkSession:
    """Create and configure a SparkSession for Structured Streaming.

    Configures the session with the Kafka connector package and sets
    appropriate defaults for streaming workloads including shuffle
    partitions and log level.

    Returns:
        A configured SparkSession instance ready for streaming operations.
    """
    spark = (
        SparkSession.builder
        .appName("ProcessingService")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        )
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        .getOrCreate()
    )

    # Reduce Spark internal logging noise
    spark.sparkContext.setLogLevel("WARN")

    return spark


def run() -> None:
    """Main execution function for the processing service.

    Creates a SparkSession, starts the streaming pipeline, and blocks
    until the query terminates (either due to an error or manual stop).
    Exits with non-zero code on unexpected termination.
    """
    logger.info("Initializing Processing Service")

    config = PipelineConfig()
    logger.info(
        f"Configuration loaded: brokers={config.bootstrap_servers}, "
        f"input={config.input_topic}, output={config.output_topic}"
    )

    spark = create_spark_session()
    logger.info("SparkSession created successfully")

    try:
        query = start_pipeline(spark, config)
        logger.info("Awaiting streaming query termination")
        query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("Received interrupt signal, shutting down gracefully")
    except Exception as e:
        logger.error(f"Pipeline terminated with error: {e}")
        sys.exit(1)
    finally:
        spark.stop()
        logger.info("SparkSession stopped")


if __name__ == "__main__":
    run()
