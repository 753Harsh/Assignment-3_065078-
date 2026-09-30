# Assignment-3_065078-# OTT Streaming Data Analytics Pipeline

A real-time streaming data analytics project for the Media & Entertainment industry. This project generates synthetic OTT viewer events, streams them through Apache Kafka, stores the events in MySQL, and visualizes streaming activity in Grafana.

## Project Architecture

```text
generate_data.py
      |
      v
ott_viewer_events.csv
      |
      v
ott_producer_updated.py
      |
      v
Apache Kafka (viewer-events topic)
      |
      v
ott_consumer_mysql.py
      |
      v
MySQL (ott_analytics database)
      |
      v
Grafana Dashboard
```

## Features

- Generates synthetic OTT viewer-event data for testing and analysis.
- Publishes event records to the Kafka topic `viewer-events`.
- Consumes Kafka messages and stores them in MySQL.
- Creates the `ott_analytics` database and `ott_viewer_events` table automatically when the consumer connects successfully.
- Supports Grafana dashboards for event trends, event-type distribution, content watch time, and device activity.

## Technology Stack

- Python
- Apache Kafka
- MySQL
- Grafana
- Docker (if used for your Kafka/MySQL services)

## Data Schema

The generated viewer-event dataset includes fields such as:

| Field | Description |
|---|---|
| `event_id` | Unique identifier for an event |
| `user_id` | Synthetic viewer identifier |
| `content_id` | Content identifier |
| `content_title` | Movie, series, or other content title |
| `content_type` | Type of content |
| `device` | Device used to stream |
| `event_type` | `play`, `pause`, `skip`, or `complete` |
| `watch_duration_sec` | Recorded watch duration in seconds |
| `total_duration_sec` | Total content duration in seconds |
| `region` | Viewer region |
| `timestamp` | Event timestamp from the generated dataset |
| `sent_at` | Time the producer sent the event |

In MySQL, the event timestamp is stored in the `event_timestamp` column, and ingestion time is tracked in `ingested_at`.

## Prerequisites

Install Python and make sure Kafka and MySQL are running and reachable from the terminal where the scripts are executed.

Install the required Python packages:

```bash
python -m pip install Faker kafka-python==2.2.15 mysql-connector-python
```

> If the `kafka` package causes import conflicts, uninstall conflicting packages and reinstall `kafka-python`:
>
> ```bash
> python -m pip uninstall kafka kafka-python -y
> python -m pip install --no-cache-dir kafka-python==2.2.15
> ```

## How to Run

Run each step in order. Keep the producer and consumer running in separate terminals where required.

### 1. Generate the sample data

```bash
python generate_data.py
```

This creates three CSV files:

- `ott_viewer_events.csv`
- `social_media_events.csv`
- `ad_events.csv`

The current producer uses `ott_viewer_events.csv`.

### 2. Start Kafka

Start your Kafka service/container and ensure the broker address is accessible. The producer and consumer scripts default to `localhost:9092`.

If Kafka runs in Docker, use the broker address that is reachable from your host or container. Container service names generally work only for containers on the same Docker network.

### 3. Start the MySQL consumer

For MySQL accessible from Windows or your local machine through port `3306`, run:

```bash
python ott_consumer_mysql.py --mysql-host localhost --mysql-port 3306 --mysql-user root --mysql-password "YOUR_MYSQL_PASSWORD"
```

Replace `YOUR_MYSQL_PASSWORD` with your own MySQL password. Do not commit real credentials to GitHub.

If the consumer runs inside the same Docker network as MySQL, use the MySQL service/container name instead, for example:

```bash
python ott_consumer_mysql.py --mysql-host cm --mysql-port 3306 --mysql-user root --mysql-password "YOUR_MYSQL_PASSWORD"
```

The consumer listens to the `viewer-events` topic and inserts received messages into MySQL. Press `Ctrl+C` to stop it.

### 4. Start the producer

In another terminal, run:

```bash
python ott_producer_updated.py
```

Optional: adjust the Kafka broker, CSV path, or event delay:

```bash
python ott_producer_updated.py --broker localhost:9092 --delay-min 0.1 --delay-max 0.4
```

To repeatedly stream the CSV data, use:

```bash
python ott_producer_updated.py --loop
```

### 5. Verify the MySQL data

Run these queries in MySQL Workbench:

```sql
USE ott_analytics;

SELECT COUNT(*) AS total_events
FROM ott_viewer_events;

SELECT *
FROM ott_viewer_events
ORDER BY event_timestamp DESC
LIMIT 20;
```

## Grafana Dashboard

Add MySQL as a Grafana data source and select the `ott_analytics` database. Use a connection host and port reachable from the Grafana server/container. If Grafana is running in Docker, `localhost` refers to the Grafana container itself, not necessarily the host machine or MySQL container.

Create a dashboard with these four panels. The queries below use Grafana's MySQL macros and the `event_timestamp` column created by the consumer.

### 1. OTT Streaming Events Over Time

**Visualization:** Time series

```sql
SELECT
    $__timeGroup(event_timestamp, '5m') AS time,
    COUNT(*) AS total_events
FROM ott_viewer_events
WHERE $__timeFilter(event_timestamp)
GROUP BY 1
ORDER BY 1;
```

### 2. Streaming Event Type Distribution

**Visualization:** Pie chart

```sql
SELECT
    event_type,
    COUNT(*) AS total_events
FROM ott_viewer_events
WHERE $__timeFilter(event_timestamp)
GROUP BY event_type
ORDER BY total_events DESC;
```

### 3. Top 10 Most-Watched Content

**Visualization:** Bar chart

```sql
SELECT
    content_title,
    ROUND(SUM(watch_duration_sec) / 60, 2) AS watch_minutes
FROM ott_viewer_events
WHERE $__timeFilter(event_timestamp)
GROUP BY content_title
ORDER BY watch_minutes DESC
LIMIT 10;
```

### 4. Streaming Activity by Device

**Visualization:** Bar chart

```sql
SELECT
    device,
    COUNT(*) AS total_events
FROM ott_viewer_events
WHERE $__timeFilter(event_timestamp)
GROUP BY device
ORDER BY total_events DESC;
```

For the first run, set the Grafana time range to **Last 7 days**. If a panel is empty, confirm that events exist in MySQL and that the selected time range includes their timestamps.

## Troubleshooting

- **Unknown MySQL host:** If scripts run on Windows and MySQL publishes port `3306` to the host, try `--mysql-host localhost`. Use a Docker service name such as `cm` only when the consumer can resolve it on the relevant Docker network.
- **Kafka connection error:** Confirm Kafka is running and the configured broker address is reachable from the producer/consumer.
- **No data in Grafana:** Check that the consumer is running, the producer is publishing messages, the MySQL table has rows, and the Grafana time range is appropriate.
- **Python import error:** Activate the intended virtual environment and install the dependencies listed above.

## Security Notes

- Never commit database passwords, API keys, or other secrets to the repository.
- Use a dedicated MySQL user with only the permissions needed by the project rather than using `root` for long-term deployments.
- If a real password has already been committed to a public repository, rotate it and remove it from the repository history where appropriate.

## Future Improvements

- Add dashboards for regional activity, content completion rates, and watch-duration trends.
- Add data validation, structured logging, and monitoring for failed messages.
- Containerize the producer and consumer and manage settings through environment variables or a secrets manager.

---

**Project:** OTT Streaming Data Analytics  
**Domain:** Media & Entertainment
