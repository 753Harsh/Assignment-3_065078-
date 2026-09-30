"""Consume OTT viewer events from Kafka and store them in MySQL for Grafana.

Matches the message schema emitted by ott_producer_updated.py:
event_id, user_id, content_id, content_title, content_type, device,
event_type, watch_duration_sec, total_duration_sec, region, timestamp, sent_at

Install dependencies:
    python -m pip install kafka-python==2.2.15 mysql-connector-python

Run (defaults):
    python ott_consumer_mysql.py

If running inside Docker, use the Kafka and MySQL service/container names that
are reachable from the consumer container, e.g. --broker kafka:9092 --mysql-host cm.
"""

import argparse
import json
import sys
import time
from datetime import datetime

TOPIC_NAME = "viewer-events"

CREATE_DATABASE_SQL = "CREATE DATABASE IF NOT EXISTS ott_analytics"
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS ott_analytics.ott_viewer_events (
    event_id VARCHAR(36) NOT NULL PRIMARY KEY,
    user_id VARCHAR(32),
    content_id VARCHAR(32),
    content_title VARCHAR(255),
    content_type VARCHAR(64),
    device VARCHAR(64),
    event_type VARCHAR(32),
    watch_duration_sec INT,
    total_duration_sec INT,
    region VARCHAR(64),
    event_timestamp DATETIME NULL,
    sent_at DATETIME NULL,
    ingested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_event_timestamp (event_timestamp),
    INDEX idx_content_title (content_title),
    INDEX idx_event_type (event_type),
    INDEX idx_region (region)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""
INSERT_SQL = """
INSERT INTO ott_analytics.ott_viewer_events (
    event_id, user_id, content_id, content_title, content_type, device,
    event_type, watch_duration_sec, total_duration_sec, region,
    event_timestamp, sent_at
) VALUES (
    %(event_id)s, %(user_id)s, %(content_id)s, %(content_title)s,
    %(content_type)s, %(device)s, %(event_type)s, %(watch_duration_sec)s,
    %(total_duration_sec)s, %(region)s, %(event_timestamp)s, %(sent_at)s
) ON DUPLICATE KEY UPDATE
    user_id=VALUES(user_id), content_id=VALUES(content_id),
    content_title=VALUES(content_title), content_type=VALUES(content_type),
    device=VALUES(device), event_type=VALUES(event_type),
    watch_duration_sec=VALUES(watch_duration_sec),
    total_duration_sec=VALUES(total_duration_sec), region=VALUES(region),
    event_timestamp=VALUES(event_timestamp), sent_at=VALUES(sent_at)
"""


def parse_datetime(value):
    """Convert producer timestamp strings to MySQL DATETIME values."""
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).replace(tzinfo=None)
    except (TypeError, ValueError):
        return None


def normalize_event(event):
    """Map a Kafka JSON event to the SQL table columns."""
    if not isinstance(event, dict):
        raise ValueError("Kafka message must be a JSON object")
    event_id = event.get("event_id")
    if not event_id:
        raise ValueError("Message is missing required field 'event_id'")

    def optional_int(value):
        if value in (None, ""):
            return None
        return int(float(value))

    return {
        "event_id": str(event_id),
        "user_id": event.get("user_id"),
        "content_id": event.get("content_id"),
        "content_title": event.get("content_title"),
        "content_type": event.get("content_type"),
        "device": event.get("device"),
        "event_type": event.get("event_type"),
        "watch_duration_sec": optional_int(event.get("watch_duration_sec")),
        "total_duration_sec": optional_int(event.get("total_duration_sec")),
        "region": event.get("region"),
        "event_timestamp": parse_datetime(event.get("timestamp")),
        "sent_at": parse_datetime(event.get("sent_at")),
    }


def connect_mysql(args):
    try:
        import mysql.connector
        from mysql.connector import Error
    except ImportError as exc:
        raise RuntimeError(
            "MySQL connector is not installed. Run: "
            "python -m pip install mysql-connector-python"
        ) from exc

    config = {
        "host": args.mysql_host,
        "port": args.mysql_port,
        "user": args.mysql_user,
        "password": args.mysql_password,
        "connection_timeout": 10,
        "autocommit": False,
    }
    try:
        # Connect without a database first so the script can create it.
        connection = mysql.connector.connect(**config)
        cursor = connection.cursor()
        cursor.execute(CREATE_DATABASE_SQL)
        cursor.execute(CREATE_TABLE_SQL)
        connection.commit()
        cursor.close()
        print(f"Connected to MySQL at {args.mysql_host}:{args.mysql_port}; "
              "database/table ott_analytics.ott_viewer_events are ready.")
        return connection
    except Error as exc:
        raise ConnectionError(
            f"Could not connect/create the MySQL database or table at "
            f"{args.mysql_host}:{args.mysql_port}: {exc}\n"
            "Check that MySQL is running, the host is reachable from this "
            "consumer, and this user has permission to create the database/table."
        ) from exc


def connect_kafka(broker, topic, group_id):
    try:
        from kafka import KafkaConsumer
        from kafka.errors import NoBrokersAvailable
    except ImportError as exc:
        raise RuntimeError(
            "Kafka Python library is not installed correctly. Run:\n"
            "python -m pip uninstall kafka kafka-python -y\n"
            "python -m pip install --no-cache-dir kafka-python==2.2.15"
        ) from exc

    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=broker,
            group_id=group_id,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            value_deserializer=lambda value: json.loads(value.decode("utf-8")),
            consumer_timeout_ms=1000,
            request_timeout_ms=30000,
            api_version_auto_timeout_ms=10000,
        )
        return consumer
    except NoBrokersAvailable as exc:
        raise ConnectionError(
            f"Could not connect to Kafka at {broker}. Check the broker address "
            "and confirm Kafka is running."
        ) from exc


def run(args):
    mysql_connection = None
    kafka_consumer = None
    cursor = None
    inserted_count = 0
    error_count = 0

    try:
        mysql_connection = connect_mysql(args)
        cursor = mysql_connection.cursor()
        kafka_consumer = connect_kafka(args.broker, args.topic, args.group_id)
        print(f"Listening to Kafka topic '{args.topic}' at {args.broker}. "
              "Press Ctrl+C to stop.")

        while True:
            received_any = False
            for message in kafka_consumer:
                received_any = True
                try:
                    row = normalize_event(message.value)
                    cursor.execute(INSERT_SQL, row)
                    mysql_connection.commit()
                    # Count successfully processed messages, including duplicate updates.
                    inserted_count += 1
                    print(
                        f"[{inserted_count}] MySQL saved: event={row['event_id']} | "
                        f"type={row['event_type']} | content={row['content_title']}"
                    )
                    # Commit Kafka offset only after the SQL transaction succeeds.
                    kafka_consumer.commit()
                except (ValueError, TypeError, json.JSONDecodeError) as exc:
                    error_count += 1
                    print(f"Skipping invalid Kafka message: {exc}", file=sys.stderr)
                    # Commit the offset for malformed messages to avoid an endless retry.
                    kafka_consumer.commit()
                except Exception as exc:
                    mysql_connection.rollback()
                    error_count += 1
                    print(f"Database/message processing error: {exc}", file=sys.stderr)
                    # Do not commit this offset; Kafka can redeliver it.
                    time.sleep(2)
                    break
            if not received_any:
                time.sleep(0.25)

    except KeyboardInterrupt:
        print("\nStopping consumer...")
    finally:
        if kafka_consumer is not None:
            try:
                kafka_consumer.close()
            except Exception:
                pass
        if cursor is not None:
            try:
                cursor.close()
            except Exception:
                pass
        if mysql_connection is not None and mysql_connection.is_connected():
            try:
                mysql_connection.close()
            except Exception:
                pass
        print(f"Consumer stopped. Successfully processed: {inserted_count}; errors: {error_count}.")


def main():
    parser = argparse.ArgumentParser(
        description="Consume OTT viewer-events from Kafka and save them to MySQL for Grafana."
    )
    parser.add_argument("--broker", default="localhost:9092",
                        help="Kafka bootstrap server (default: localhost:9092)")
    parser.add_argument("--topic", default=TOPIC_NAME,
                        help=f"Kafka topic (default: {TOPIC_NAME})")
    parser.add_argument("--group-id", default="ott-mysql-consumer",
                        help="Kafka consumer group ID")
    parser.add_argument("--mysql-host", default="cm",
                        help="MySQL hostname/container name (default: cm)")
    parser.add_argument("--mysql-port", type=int, default=3306,
                        help="MySQL port (default: 3306)")
    parser.add_argument("--mysql-user", default="root",
                        help="MySQL username (default: root)")
    parser.add_argument("--mysql-password", default="Harsh7503@",
                        help="MySQL password; preferably override with MYSQL_PASSWORD env var")
    args = parser.parse_args()
    # An environment variable can override the embedded convenience default.
    import os
    args.mysql_password = os.getenv("MYSQL_PASSWORD", args.mysql_password)

    try:
        run(args)
    except (RuntimeError, ConnectionError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
