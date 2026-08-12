"""
Generate the architecture flow chart for the Real-Time Data Backend.
Produces: Frohnholzer-Madeleine_14090766_DLMDSEDE02_P1_S.png

Fix v3:
- Strict left-to-right linear flow: Ingestion → raw_data → Processing → aggregated_data → Grafana → Browser
- Security layers integrated into the main network cluster label (avoids Graphviz floating cluster issue)
"""
from diagrams import Cluster, Diagram, Edge
from diagrams.onprem.queue import Kafka
from diagrams.onprem.analytics import Spark
from diagrams.onprem.monitoring import Grafana
from diagrams.programming.language import Python
from diagrams.onprem.client import User

# Output filename (without extension — diagrams adds .png automatically)
OUTPUT_NAME = "Frohnholzer-Madeleine_14090766_DLMDSEDE02_P1_S"

FONT = "Helvetica Neue"
FONT_BOLD = "Helvetica Neue Bold"

graph_attr = {
    "fontsize": "20",
    "bgcolor": "white",
    "pad": "1.0",
    "dpi": "200",
    "labelloc": "t",
    "labeljust": "c",
    "label": "Real-Time Data Backend — System Architecture\n"
             "Streaming-native pipeline | Docker Compose | Defense-in-Depth Security\n\n"
             "Security Layers:  "
             "① Network: Docker internal network, only port 3000 exposed  |  "
             "② Application: SASL/PLAIN auth, per-service Kafka ACLs (least privilege)  |  "
             "③ Data: Column-level exclusion filtering, row-level inclusion filtering",
    "fontname": FONT_BOLD,
    "rankdir": "LR",
    "nodesep": "1.0",
    "ranksep": "1.5",
}

with Diagram(
    "",
    filename=f"docs/{OUTPUT_NAME}",
    show=False,
    direction="LR",
    graph_attr=graph_attr,
    outformat="png",
):
    # External user accessing Grafana
    user = User("Host Browser\nlocalhost:3000")

    with Cluster(
        "Internal Docker Network (data-net)\n"
        "internal: true | restart: on-failure:5 | health checks | docker-compose up",
        graph_attr={
            "style": "dashed",
            "color": "#333333",
            "fontsize": "15",
            "fontname": FONT_BOLD,
            "bgcolor": "#f9f9f9",
        },
    ):

        # Ingestion Service
        with Cluster(
            "Ingestion_Service\nPython 3.11 + Faker + confluent-kafka",
            graph_attr={
                "style": "rounded",
                "color": "#6c8ebf",
                "bgcolor": "#dae8fc",
                "fontsize": "14",
                "fontname": FONT_BOLD,
            },
        ):
            ingestion = Python("Data Generator\n1M+ IoT records\n100-10K rec/s")

        # Kafka topics
        with Cluster(
            "Message Broker — Apache Kafka 3.7.0\nKRaft mode | 2 Brokers | SASL/PLAIN | Per-Service ACLs",
            graph_attr={
                "style": "rounded",
                "color": "#b85450",
                "bgcolor": "#f8cecc",
                "fontsize": "14",
                "fontname": FONT_BOLD,
            },
        ):
            raw_topic = Kafka("raw_data\n3 part | RF=2\nretention: 24h")
            agg_topic = Kafka("aggregated_data\n3 part | RF=2\nretention: 7d")

        # Processing Service
        with Cluster(
            "Processing_Service\nPySpark Structured Streaming | Checkpointing",
            graph_attr={
                "style": "rounded",
                "color": "#82b366",
                "bgcolor": "#d5e8d4",
                "fontsize": "14",
                "fontname": FONT_BOLD,
            },
        ):
            processing = Spark(
                "Sliding Window Agg\n5 min / 60s slide\n"
                "count, avg, sum\nby canton"
            )

        # Visualization Service
        with Cluster(
            "Visualization_Service\nGrafana OSS 10.4.2 + Kafka Plugin",
            graph_attr={
                "style": "rounded",
                "color": "#9673a6",
                "bgcolor": "#e1d5e7",
                "fontsize": "14",
                "fontname": FONT_BOLD,
            },
        ):
            grafana = Grafana("Live Dashboards\nTemp/Humidity/Pressure\nby Canton")

    # === LINEAR DATA FLOW (left to right) ===
    # 1. Ingestion → raw_data
    ingestion >> Edge(
        label="produce (SASL/PLAIN)\nKey: device_id",
        color="#6c8ebf",
        style="bold",
        fontsize="13",
        fontname=FONT,
    ) >> raw_topic

    # 2. raw_data → Processing
    raw_topic >> Edge(
        label="consume (SASL/PLAIN)\ngroup: processing-group",
        color="#82b366",
        style="bold",
        fontsize="13",
        fontname=FONT,
    ) >> processing

    # 3. Processing → aggregated_data
    processing >> Edge(
        label="produce (SASL/PLAIN)\nKey: canton",
        color="#82b366",
        style="bold",
        fontsize="13",
        fontname=FONT,
    ) >> agg_topic

    # 4. aggregated_data → Grafana
    agg_topic >> Edge(
        label="consume (SASL/PLAIN)\nKafka Data Source Plugin",
        color="#9673a6",
        style="bold",
        fontsize="13",
        fontname=FONT,
    ) >> grafana

    # 5. Grafana → Host Browser
    grafana >> Edge(
        label="Port 3000\n(only exposed port)",
        color="#333333",
        style="dashed",
        fontsize="13",
        fontname=FONT_BOLD,
    ) >> user
