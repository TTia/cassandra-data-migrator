# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Cassandra Data Migrator (CDM) is a DataStax-sponsored Apache Spark-based tool for migrating and validating data between Cassandra clusters, with PostgreSQL target support. It's a production-grade tool with distributed processing, SSL support, rate limiting, and auto-correction capabilities.

## Build & Development

```bash
# Build fat JAR (requires Maven 3.9.x, Java 11+)
mvn clean package
# Output: target/cassandra-data-migrator-5.7.3-SNAPSHOT.jar

# Run tests with coverage
mvn test

# Compile Scala only
mvn scala:compile
```

**Tech Stack:** Java 11 (target) + Scala 2.13 + Spark 3.5.7 + Cassandra Driver 4.19.2

## Running Jobs

```bash
# Migration: Cassandra → Cassandra/PostgreSQL
spark-submit --class com.datastax.cdm.job.Migrate \
  --master "local[*]" --driver-memory 25G --executor-memory 25G \
  cassandra-data-migrator-5.7.3.jar cdm.properties

# Validation: Compare origin vs target, optionally auto-correct
spark-submit --class com.datastax.cdm.job.DiffData \
  cassandra-data-migrator-5.7.3.jar cdm.properties

# Guardrail Check: Find large fields that violate cluster limits
spark-submit --class com.datastax.cdm.job.GuardrailCheck \
  cassandra-data-migrator-5.7.3.jar cdm.properties
```

## Architecture

```
com.datastax.cdm/
├── job/                    # Entry points & job execution
│   ├── Migrate.scala       # Main migration job
│   ├── DiffData.scala      # Validation/diff job
│   ├── BasePartitionJob.scala
│   ├── CopyJobSession.java           # Cassandra→Cassandra
│   ├── PostgresCopyJobSession.java   # Cassandra→PostgreSQL
│   ├── DiffJobSession.java           # Cassandra validation
│   └── PostgresDiffJobSession.java   # PostgreSQL validation
│
├── schema/                 # Schema handling
│   ├── CqlTable.java       # Cassandra table metadata
│   ├── PostgresTable.java  # PostgreSQL table metadata
│   └── PostgresTypeMapper.java  # Type conversion rules
│
├── cql/                    # Statement building
│   ├── statement/
│   │   ├── OriginSelect*.java       # Origin queries
│   │   ├── Target*.java             # Cassandra target ops
│   │   └── Postgres*.java           # PostgreSQL ops
│   └── codec/              # ~20 type codecs for conversions
│
├── data/                   # Data handling
│   ├── PKFactory.java      # Primary key management
│   └── CqlConversion.java  # Type conversions
│
├── feature/                # Data transformations
│   ├── TrackRun.java       # Run tracking & metrics
│   ├── WritetimeTTL.java   # Preserve Cassandra writetimes/TTLs
│   ├── ConstantColumns.java
│   ├── ExplodeMap.java     # Expand MAP to multiple rows
│   └── ExtractJson.java    # Extract values from JSON columns
│
├── connect/
│   └── PostgresConnectionFactory.java  # HikariCP pooling
│
└── properties/
    └── KnownProperties.java  # All ~200+ config properties
```

## Key Patterns

**Job Execution Flow:**
1. Scala entry point (`Migrate.scala`) → creates Spark context
2. Partitions origin data by Cassandra token ranges
3. Each executor processes partitions via `*JobSession` classes
4. Sessions handle connection management, statement execution, metrics

**PostgreSQL Integration:**
- `spark.cdm.connect.target.type=postgres` switches target
- Collections/UDTs → JSONB, primitives map directly
- Uses prepared statements with UPSERT (ON CONFLICT UPDATE)

**Configuration:** Properties files in `src/resources/`:
- `cdm.properties` - Simplified config
- `cdm-detailed.properties` - Full reference (~200+ properties)
- `cdm-postgres.properties` - PostgreSQL-specific

## Testing

```bash
# Run all tests
mvn test

# Run specific test class
mvn test -Dtest=PostgresCopyJobSessionTest

# Run with debug output
mvn test -Dtest=CassandraToPostgresIntegrationTest -DfailIfNoTests=false
```

**Test Stack:** JUnit 5 + Mockito 5 + Testcontainers (TimescaleDB for PostgreSQL tests)

Integration tests use embedded Cassandra 5.0.6 and containerized databases.

## Important Conventions

- **Java 11 compatibility required:** No pattern matching, text blocks, switch expressions, or `Stream.toList()`
- **Spark serialization:** Job sessions must be serializable; use `transient` for non-serializable fields
- **Rate limiting:** Per-executor values; divide by number of workers for cluster deployments
- **Batch operations:** Use `batchSize=1` when primary-key equals partition-key or rows >20KB
