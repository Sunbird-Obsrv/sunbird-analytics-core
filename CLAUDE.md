# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Sunbird Analytics Core is an Apache Spark-based analytics framework in Scala. It implements a pluggable, three-stage data product architecture for batch and streaming processing across multiple cloud storage backends (AWS, Azure, GCP, OCI, Ceph).

## Build & Test Commands

This is a Maven multi-module project. Cloud store dependency must be specified via system properties.

**Build (skip tests):**
```
mvn clean install -DskipTests -DCLOUD_STORE_GROUP_ID=org.sunbird -DCLOUD_STORE_ARTIFACT_ID=cloud-storage-sdk-aws -DCLOUD_STORE_VERSION=2.0.0
```

**Run all tests with coverage:**
```
mvn scoverage:report -DCLOUD_STORE_GROUP_ID=org.sunbird -DCLOUD_STORE_ARTIFACT_ID=cloud-storage-sdk-aws -DCLOUD_STORE_VERSION=2.0.0
```

**Run tests for a single module:**
```
mvn test -pl analytics-core -DCLOUD_STORE_GROUP_ID=org.sunbird -DCLOUD_STORE_ARTIFACT_ID=cloud-storage-sdk-aws -DCLOUD_STORE_VERSION=2.0.0
```

**Run a single test class:**
```
mvn test -pl analytics-core -Dsuites="org.ekstep.analytics.framework.TestClassName" -DCLOUD_STORE_GROUP_ID=org.sunbird -DCLOUD_STORE_ARTIFACT_ID=cloud-storage-sdk-aws -DCLOUD_STORE_VERSION=2.0.0
```

**Requires:** Java 11, Maven 3+

## Modules

- **analytics-core** — Core framework: interfaces, dispatchers, fetchers, filters, utilities
- **analytics-job-driver** — Batch and streaming job entry points (produces shaded uber JAR `analytics-framework-2.0.jar`)
- **batch-models** — Concrete batch data product implementations (workflow summary, monitoring, Druid query processing, etc.)
- **optional-master** — Local third-party command-line option parsing library (not modified)

## Architecture

### Three-Stage Data Product Pattern

All data products extend `IBatchModelTemplate[T, A, B, R]` with three phases:
1. **preProcess** — Data transformation, filtering, joining
2. **algorithm** — Core computation/analysis
3. **postProcess** — Result formatting, persistence

### Configuration-Driven Jobs

Jobs are configured via `JobConfig` which composes:
- `Fetcher` — Data source (S3, Azure, GCloud, Druid, local)
- `Filter[]` — Data selection predicates
- `model` — Fully qualified model class name
- `modelParams` — Model-specific parameters
- `Dispatcher[]` — Output destinations (S3, Kafka, File, Elasticsearch, etc.)

### Key Abstractions

- `IBatchModel` / `IBatchModelTemplate` — Data product interfaces (`analytics-core/src/main/scala/org/ekstep/analytics/framework/`)
- `IDispatcher` — Output dispatchers (`framework/dispatcher/`)
- Fetchers — Data source readers (`framework/fetcher/`)
- `IMatcher` — Filtering predicates (`framework/filter/`)
- `JobDriver` / `BatchJobDriver` / `StreamingJobDriver` — Job orchestration (`analytics-job-driver`)

### Data Models

Core event types in `analytics-core`: `Event` (raw telemetry), `DerivedEvent` (computed), `MeasuredEvent` (final output), `Dimensions` (dimensional data).

## Tech Stack

- **Scala 2.12.10**, **Spark 3.2.1**, **Hadoop 3.2.0**
- **Cassandra** (Spark connector 3.1.0), **Neo4j**, **Druid** (Scruid 2.5.0), **Kafka**, **Elasticsearch**
- **Jackson 2.12.0** and **Circe 0.13.0** for JSON
- **Akka** for actor model
- **ScalaTest 3.0.5** + **ScalaMock 4.1.0** for testing

## Testing

Tests use embedded services: `EmbeddedPostgresqlService`, `EmbeddedCassandra`, `EmbeddedES`, and `scalatest-embedded-kafka`. Test base classes:
- `BaseSpec` — Basic ScalaTest `FlatSpec with Matchers`
- `SparkSpec(file)` — Initializes SparkContext and loads test telemetry data

Test data (sample telemetry logs) lives in each module's `src/test/resources/`.

## Package Structure

All source code is under the `org.ekstep.analytics` package namespace, primarily `org.ekstep.analytics.framework`.
