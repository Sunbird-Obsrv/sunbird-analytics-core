# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Sunbird Analytics Core is an Apache Spark-based analytics framework in Scala. It implements a pluggable, three-stage data product architecture for batch and streaming processing with AWS S3 cloud storage backend.

## Build & Test Commands

This is a Maven multi-module project. Cloud store dependency must be specified via system properties. **Requires Java 11** (not 17+; Spark 3.2.x has module access issues on Java 17).

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

## Modules

- **analytics-core** — Core framework: interfaces, dispatchers, fetchers, filters, utilities
- **analytics-job-driver** — Batch and streaming job entry points (produces shaded uber JAR `analytics-framework-2.0.jar`)
- **batch-models** — Concrete batch data product implementations (workflow summary, monitoring, etc.)
- **optional-master** — Local third-party command-line option parsing library (not modified)

## Architecture

### Three-Stage Data Product Pattern

All data products extend `IBatchModelTemplate[T, A, B, R]` with three phases:
1. **preProcess** — Data transformation, filtering, joining
2. **algorithm** — Core computation/analysis
3. **postProcess** — Result formatting, persistence

Type parameters map to: `[InputRDD element type, preProcess output, algorithm output, postProcess output]`. Concrete models are singleton `object`s (not classes) that also extend `Serializable`. Model params arrive as `Map[String, AnyRef]`; use `.getOrElse` with defaults.

### Configuration-Driven Jobs

Jobs are configured via `JobConfig` which composes:
- `Fetcher` — Data source (`"s3"`/`"aws"`, `"local"`, or `"none"`)
- `Filter[]` — Data selection predicates
- `model` — Fully qualified model class name
- `modelParams` — Model-specific parameters
- `Dispatcher[]` — Output destinations (`"s3"`, `"kafka"`, `"file"`, `"console"`)

### Key Abstractions

- `IBatchModel` / `IBatchModelTemplate` — Data product interfaces (`analytics-core/src/main/scala/org/ekstep/analytics/framework/`)
- `IDispatcher` — Output dispatchers: S3Dispatcher, KafkaDispatcher, FileDispatcher, ConsoleDispatcher, HadoopDispatcher (`framework/dispatcher/`)
- `S3DataFetcher` — S3/AWS data reader; replaces `s3n://` with `s3a://` automatically (`framework/fetcher/`)
- `DataFilter` / `IMatcher` — Filtering predicates (`framework/filter/`)
- `JobDriver` / `BatchJobDriver` / `StreamingJobDriver` — Job orchestration (`analytics-job-driver`)

### Context Objects

- **`FrameworkContext`** (passed as implicit) — holds lazy-loaded `IStorageService` instances and `inputEventsCount`/`outputEventsCount` LongAccumulators. Jobs must initialize accumulators before use; tests call `fc.inputEventsCount.reset()` between assertions.
- **`JobContext`** (singleton object) — holds `parallelization: Int = 10` and `rddList` for memory management. Call `JobContext.recordRDD(rdd)` on each stage RDD; `JobContext.cleanUpRDDs()` in `afterAll`.

### Data Models

Core event types in `analytics-core`: `Event` (raw telemetry), `DerivedEvent` (computed), `MeasuredEvent` (final output), `Dimensions` (dimensional data).

### DataFilter Matching

`DataFilter.filter[T](events, filters)` uses short-circuit evaluation. `Filter.name` is a bean property path with special aliases: `"eventId"→eid`, `"ts"`, `"eventts"`, `"userId"→uid`, `"sessionId"→sid`, `"telemetryVersion"→ver`. Matchers: `InMatcher`, `RangeMatcher`, `NotEmptyMatcher`, `EqualsMatcher`.

## Tech Stack

- **Scala 2.12.10**, **Spark 3.2.1**, **Hadoop 3.4.0**
- **Kafka 3.5.2**, **Cassandra** (deprecated, legacy configs only)
- **Jackson 2.12.7** (databind pinned to 2.12.7.1), **Circe 0.13.0** for JSON
- **Log4j 2.25.3** for logging
- **ScalaTest 3.0.5** + **ScalaMock 4.1.0** for testing

## Deployment

Both `analytics-job-driver` and `batch-models` produce shaded uber JARs via maven-shade-plugin. The shaded JARs exclude:
- Jackson classes (`com/fasterxml/jackson/**`) — provided by Spark
- SLF4J classes (`org/slf4j/**`) — provided by Spark's classpath to avoid classloader conflicts

The production Spark cluster runs at `/data/analytics/spark-3.1.3-bin-hadoop3.2/` with application JARs deployed to `/data/analytics/models-2.0/`.

## Testing

Test base classes:
- `BaseSpec` — Basic ScalaTest `FlatSpec with Matchers`
- `SparkSpec` — Initializes SparkContext and loads test telemetry data

Embedded Kafka uses `io.github.embeddedkafka` 3.5.1 for integration tests. Test data lives in `src/test/resources/` organized by model name. Set `cloud_storage_type = "local"` in test `application.conf` to avoid cloud SDK calls.

## Package Structure

All source code is under the `org.ekstep.analytics` package namespace, primarily `org.ekstep.analytics.framework`.
