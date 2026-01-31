
# Real-Time Streaming Data Pipeline with Apache Kafka and Spark Streaming

This project implements a real-time streaming data pipeline using Apache Kafka, Apache Spark Structured Streaming, and PostgreSQL. It simulates user activity events, processes them in real time, stores aggregated metrics in PostgreSQL, writes raw events to a Parquet-based data lake, and publishes enriched events to another Kafka topic. [page:0][web:27]

## Architecture

Components:

- Zookeeper: coordinates the Kafka broker.
- Kafka: message broker with topics:
  - `user_activity` (input events)
  - `enriched_activity` (output enriched events)
- PostgreSQL: stores aggregated metrics in three tables:
  - `page_view_counts`
  - `active_users`
  - `user_sessions`
- Spark Structured Streaming:
  - Reads from Kafka topic `user_activity`
  - Computes time-windowed metrics
  - Writes results to PostgreSQL and to a data lake (Parquet)
  - Publishes enriched events to Kafka topic `enriched_activity`
- Python producer script:
  - Simulates user activity events and sends them to Kafka. [page:0][web:27]

All services are orchestrated via Docker Compose.

## Prerequisites

- Docker and Docker Compose installed
- Python 3.x installed on host (for running the producer script)
- Git (for version control and pushing to GitHub)

## Project Structure

Key files and directories:

- `docker-compose.yml` – defines Zookeeper, Kafka, PostgreSQL, and Spark services.
- `init-db.sql` – SQL script to create required PostgreSQL tables.
- `.env.example` – sample environment variables for PostgreSQL credentials.
- `spark/Dockerfile` – Spark image extension to run the streaming job.
- `spark/app/main.py` – Spark Structured Streaming job.
- `scripts/producer.py` – Python script that produces user activity events into Kafka.
- `data/lake/` – local folder used as a Parquet data lake (mounted into Spark container). [page:0]

## Database Schema

The PostgreSQL database contains three tables created via `init-db.sql`:

```sql
CREATE TABLE page_view_counts (
    window_start TIMESTAMP NOT NULL,
    window_end   TIMESTAMP NOT NULL,
    page_url     TEXT      NOT NULL,
    view_count   BIGINT,
    PRIMARY KEY (window_start, page_url)
);

CREATE TABLE active_users (
    window_start        TIMESTAMP NOT NULL,
    window_end          TIMESTAMP NOT NULL,
    active_user_count   BIGINT,
    PRIMARY KEY (window_start)
);

CREATE TABLE user_sessions (
    user_id                  TEXT PRIMARY KEY,
    session_start_time       TIMESTAMP,
    session_end_time         TIMESTAMP,
    session_duration_seconds BIGINT
);
