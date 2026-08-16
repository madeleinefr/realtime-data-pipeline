#!/bin/bash
# =============================================================================
# Kafka Topic Initialization Script
# =============================================================================
# Creates required topics and configures ACLs for the real-time data pipeline.
#
# Topics:
#   - raw_data:        Ingested sensor data (3 partitions, RF=2, 24h retention)
#   - aggregated_data: Processed aggregations (3 partitions, RF=2, 7d retention)
#
# ACLs (least-privilege per service):
#   - ingestion:     WRITE raw_data
#   - processing:    READ raw_data, READ processing-group, WRITE aggregated_data
#   - visualization: READ aggregated_data, READ visualization-group
#
# Prerequisites:
#   - Kafka brokers running in KRaft mode with SASL/PLAIN authentication
#   - Admin credentials available (super user bypasses ACL checks)
#   - kafka-topics.sh and kafka-acls.sh available on PATH
#
# Usage:
#   ./create-topics.sh [BOOTSTRAP_SERVER]
#
# Environment variables:
#   BOOTSTRAP_SERVER - Kafka bootstrap server (default: kafka1:9092)
# =============================================================================

set -euo pipefail

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BOOTSTRAP_SERVER="${1:-${BOOTSTRAP_SERVER:-kafka1:9092}}"

# Admin credentials for SASL/PLAIN authentication
ADMIN_USER="admin"
ADMIN_PASSWORD="admin-secret"

# Common command config for SASL authentication
COMMAND_CONFIG_FILE="/tmp/admin-client.properties"

# Topic configuration
RAW_DATA_TOPIC="raw_data"
RAW_DATA_PARTITIONS=3
RAW_DATA_REPLICATION_FACTOR=2
RAW_DATA_RETENTION_MS=86400000        # 24 hours

AGGREGATED_DATA_TOPIC="aggregated_data"
AGGREGATED_DATA_PARTITIONS=3
AGGREGATED_DATA_REPLICATION_FACTOR=2
AGGREGATED_DATA_RETENTION_MS=604800000  # 7 days

# Retry configuration for broker health check
MAX_RETRIES=30
RETRY_INTERVAL=2

# ---------------------------------------------------------------------------
# Helper Functions
# ---------------------------------------------------------------------------

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

error() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: $1" >&2
}

# ---------------------------------------------------------------------------
# Create SASL client properties file
# ---------------------------------------------------------------------------

create_client_properties() {
    log "Creating admin client properties file..."
    cat > "${COMMAND_CONFIG_FILE}" <<EOF
security.protocol=SASL_PLAINTEXT
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required \
    username="${ADMIN_USER}" \
    password="${ADMIN_PASSWORD}";
EOF
    log "Client properties file created at ${COMMAND_CONFIG_FILE}"
}

# ---------------------------------------------------------------------------
# Wait for Kafka brokers to be available
# ---------------------------------------------------------------------------

wait_for_brokers() {
    log "Waiting for Kafka brokers to be available at ${BOOTSTRAP_SERVER}..."

    local retries=0
    while [ $retries -lt $MAX_RETRIES ]; do
        if kafka-metadata.sh --snapshot /tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log --cluster-id 2>/dev/null || \
           kafka-topics.sh --bootstrap-server "${BOOTSTRAP_SERVER}" \
               --command-config "${COMMAND_CONFIG_FILE}" \
               --list >/dev/null 2>&1; then
            log "Kafka brokers are available."
            return 0
        fi

        retries=$((retries + 1))
        log "Brokers not ready yet (attempt ${retries}/${MAX_RETRIES}). Retrying in ${RETRY_INTERVAL}s..."
        sleep "${RETRY_INTERVAL}"
    done

    error "Kafka brokers did not become available after $((MAX_RETRIES * RETRY_INTERVAL))s"
    return 1
}

# ---------------------------------------------------------------------------
# Create Topics
# ---------------------------------------------------------------------------

create_topic() {
    local topic_name="$1"
    local partitions="$2"
    local replication_factor="$3"
    local retention_ms="$4"

    log "Creating topic '${topic_name}' (partitions=${partitions}, RF=${replication_factor}, retention=${retention_ms}ms)..."

    # Check if topic already exists
    if kafka-topics.sh --bootstrap-server "${BOOTSTRAP_SERVER}" \
        --command-config "${COMMAND_CONFIG_FILE}" \
        --describe --topic "${topic_name}" >/dev/null 2>&1; then
        log "Topic '${topic_name}' already exists. Skipping creation."
        return 0
    fi

    kafka-topics.sh --bootstrap-server "${BOOTSTRAP_SERVER}" \
        --command-config "${COMMAND_CONFIG_FILE}" \
        --create \
        --topic "${topic_name}" \
        --partitions "${partitions}" \
        --replication-factor "${replication_factor}" \
        --config retention.ms="${retention_ms}"

    if [ $? -eq 0 ]; then
        log "Topic '${topic_name}' created successfully."
    else
        error "Failed to create topic '${topic_name}'."
        return 1
    fi
}

# ---------------------------------------------------------------------------
# Set up ACLs
# ---------------------------------------------------------------------------

setup_acls() {
    log "Setting up ACLs..."

    # --- Ingestion_Service: WRITE to raw_data ---
    log "Granting ingestion user WRITE access to '${RAW_DATA_TOPIC}'..."
    kafka-acls.sh --bootstrap-server "${BOOTSTRAP_SERVER}" \
        --command-config "${COMMAND_CONFIG_FILE}" \
        --add \
        --allow-principal User:ingestion \
        --operation Write \
        --topic "${RAW_DATA_TOPIC}"

    # --- Processing_Service: READ raw_data, READ consumer group, WRITE aggregated_data ---
    log "Granting processing user READ access to '${RAW_DATA_TOPIC}'..."
    kafka-acls.sh --bootstrap-server "${BOOTSTRAP_SERVER}" \
        --command-config "${COMMAND_CONFIG_FILE}" \
        --add \
        --allow-principal User:processing \
        --operation Read \
        --topic "${RAW_DATA_TOPIC}"

    log "Granting processing user READ access to consumer group 'processing-group'..."
    kafka-acls.sh --bootstrap-server "${BOOTSTRAP_SERVER}" \
        --command-config "${COMMAND_CONFIG_FILE}" \
        --add \
        --allow-principal User:processing \
        --operation Read \
        --group processing-group

    log "Granting processing user WRITE access to '${AGGREGATED_DATA_TOPIC}'..."
    kafka-acls.sh --bootstrap-server "${BOOTSTRAP_SERVER}" \
        --command-config "${COMMAND_CONFIG_FILE}" \
        --add \
        --allow-principal User:processing \
        --operation Write \
        --topic "${AGGREGATED_DATA_TOPIC}"

    # --- Visualization_Service: READ aggregated_data, READ consumer group ---
    log "Granting visualization user READ access to '${AGGREGATED_DATA_TOPIC}'..."
    kafka-acls.sh --bootstrap-server "${BOOTSTRAP_SERVER}" \
        --command-config "${COMMAND_CONFIG_FILE}" \
        --add \
        --allow-principal User:visualization \
        --operation Read \
        --topic "${AGGREGATED_DATA_TOPIC}"

    log "Granting visualization user READ access to consumer group 'visualization-group'..."
    kafka-acls.sh --bootstrap-server "${BOOTSTRAP_SERVER}" \
        --command-config "${COMMAND_CONFIG_FILE}" \
        --add \
        --allow-principal User:visualization \
        --operation Read \
        --group visualization-group

    log "ACLs configured successfully."
}

# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------

cleanup() {
    if [ -f "${COMMAND_CONFIG_FILE}" ]; then
        rm -f "${COMMAND_CONFIG_FILE}"
        log "Cleaned up temporary client properties file."
    fi
}

trap cleanup EXIT

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

main() {
    log "=========================================="
    log "Kafka Topic Initialization"
    log "Bootstrap server: ${BOOTSTRAP_SERVER}"
    log "=========================================="

    # Step 1: Create client properties for SASL authentication
    create_client_properties

    # Step 2: Wait for brokers to be healthy
    wait_for_brokers

    # Step 3: Create topics
    create_topic "${RAW_DATA_TOPIC}" "${RAW_DATA_PARTITIONS}" "${RAW_DATA_REPLICATION_FACTOR}" "${RAW_DATA_RETENTION_MS}"
    create_topic "${AGGREGATED_DATA_TOPIC}" "${AGGREGATED_DATA_PARTITIONS}" "${AGGREGATED_DATA_REPLICATION_FACTOR}" "${AGGREGATED_DATA_RETENTION_MS}"

    # Step 4: Set up ACLs
    setup_acls

    log "=========================================="
    log "Topic initialization complete!"
    log "=========================================="
}

main "$@"
