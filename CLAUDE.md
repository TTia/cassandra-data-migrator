# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Cassandra Data Migrator (CDM) is DataStax's Apache Spark tool for migrating and validating data
between Cassandra clusters. This fork adds PostgreSQL as a target. User docs: `README.md` for
Cassandra targets, `docs/POSTGRESQL_TARGET.md` for PostgreSQL (type mapping, pool and write
settings, tuning).

**Tech stack:** Java 11 target, Scala 2.13, Spark 3.5.7, Cassandra Java Driver 4.19.2. Versions are
in the `pom.xml` properties.

## Build

```bash
mvn clean package   # fat jar: target/cassandra-data-migrator-<version>.jar (Maven 3.9.x)
mvn scala:compile   # Scala only
```

Build side effects:
- `formatter-maven-plugin` (`format`) and `impsort-maven-plugin` (`sort`) run on every build and
  rewrite Java sources in place. Commit the reformatted output; `-Dformatter.skip -Dimpsort.skip`
  turns them off. Both are pinned to the last versions that run on Java 11.
- `apache-rat-plugin` checks license headers at `verify`. A new file needs the Apache license
  header or an entry in `rat-excludes.txt`.
- JaCoCo `check` runs in the `test` phase with bundle-wide coverage thresholds, so a `-Dtest=...`
  run fails the build even when every test passes. Add `-Djacoco.skip=true` to single-test runs.
  `prepare-agent` appends to `target/jacoco.exec`, so leftover data from earlier runs can hide or
  change the result.

## Running jobs

```bash
# Migration: Cassandra -> Cassandra or PostgreSQL
spark-submit --class com.datastax.cdm.job.Migrate \
  --master "local[*]" --driver-memory 25G --executor-memory 25G \
  cassandra-data-migrator-<version>.jar cdm.properties

# Validation: compare origin and target, optionally auto-correct
spark-submit --class com.datastax.cdm.job.DiffData cassandra-data-migrator-<version>.jar cdm.properties

# Guardrail check: find large fields (origin only)
spark-submit --class com.datastax.cdm.job.GuardrailCheck cassandra-data-migrator-<version>.jar cdm.properties
```

Config templates are in `src/resources/`: `cdm.properties`, `cdm-detailed.properties` (full
reference), `cdm-postgres.properties`, `cdm-postgres-detailed.properties`.

## Testing

```bash
mvn test                                 # unit tests
mvn test -Dtest=PostgresTypeMapperTest -Djacoco.skip=true   # one class
```

- JUnit 5 and Mockito 5. Tests that need a database use Testcontainers
  (`timescale/timescaledb:latest-pg16`, plus `cassandra:4.1` in `PostgresJobSessionTest`).
- Those classes are annotated `@Testcontainers(disabledWithoutDocker = true)`: they run whenever
  Docker is up and are skipped silently when it is not. Coverage from them counts toward the
  JaCoCo gate, so a run without Docker can fail the gate.
- `PostgresJobSessionTest` is the only test that runs a real session end to end (Cassandra origin
  -> PostgreSQL target, copy and diff). The Cassandra-to-Cassandra sessions are covered by SIT.

SIT is the Docker-based end-to-end harness that `cdm-integrationtest.yml` runs. Scenarios live in
`SIT/smoke`, `SIT/regression` and `SIT/features`:

```bash
cd SIT && make     # build the jar, start Cassandra in Docker, run all three suites, tear down
make test_smoke    # likewise test_regression, test_features; test_local is not run in CI
```

## Architecture

The job flow spans the Scala entry points and the Java sessions:

1. `Migrate`, `DiffData` and `GuardrailCheck` (`src/main/scala/com/datastax/cdm/job/`) extend
   `BaseJob`, which builds the Spark context and `PropertyHelper` from the properties file.
2. `Migrate` and `DiffData` choose the session factory from `spark.cdm.connect.target.type`.
   `postgres` or `postgresql` selects `PostgresCopyJobSessionFactory` /
   `PostgresDiffJobSessionFactory`; anything else (default `cassandra`) selects
   `CopyJobSessionFactory` / `DiffJobSessionFactory`. This is the only place the target type
   switches.
3. `BasePartitionJob.getParts` splits the token range with `SplitPartitions`. On a rerun
   (previous run id, or auto-rerun) it loads the pending partitions from `TrackRun` instead.
4. The factory is broadcast. On each executor, `getInstance(...)` returns a session that the
   factory holds as a static singleton, one per JVM. The session then runs
   `processPartitionRange` for each `PartitionRange`, counting into `JobCounter`.

PostgreSQL target:
- The Postgres factories ignore the target `CqlSession`. Writes go over JDBC through
  `connect/PostgresConnectionFactory` (HikariCP).
- `schema/PostgresTable` reads target metadata, `schema/PostgresTypeMapper` converts CQL values
  (collections and UDTs become JSONB), and `cql/statement/PostgresUpsertStatement` builds
  `INSERT ... ON CONFLICT` upserts.
- There is no target `CqlTable`, so the Postgres sessions pass the origin table as its own
  "other" table (`setOtherCqlTable(origin)`) and as the `PKFactory` target. `PKFactory` reads the
  corresponding indexes on every row, so both calls are needed.
- Features are only initialized when there is a Cassandra target (`AbstractJobSession`), so on
  this path `ConstantColumns`, `ExplodeMap`, `ExtractJson` and `WritetimeTTL` are off, and
  `spark.cdm.filter.cassandra.whereCondition` must start with `AND`.

Elsewhere: `properties/KnownProperties.java` registers every `spark.cdm.*` property with its type
and default. `feature/` holds the transforms (`ConstantColumns`, `ExplodeMap`, `ExtractJson`,
`WritetimeTTL`) and `TrackRun`. `cql/codec/` holds the type codecs.

## Repo context

- `origin` is GitHub (`TTia/cassandra-data-migrator`, forked from
  `datastax/cassandra-data-migrator`) and CI is GitHub Actions, so use `gh` here, not `glab`.
- `maven.yml` (package, Testcontainers tests included) and `cdm-integrationtest.yml` (SIT) run on
  PRs and pushes to `main` (JDK 11, 17, 21, 25). A branch with no open PR gets no CI.
- `test-backup/` holds pre-4.0 tests and is not compiled.

## Conventions

- Java 11 compatibility: no pattern matching, text blocks, switch expressions or
  `Stream.toList()`.
- Spark serialization: session factories are broadcast, so they must be `Serializable`. Sessions
  are never serialized. Each factory builds its session on the executor and keeps it in a static
  field, so connections and other non-serializable state belong in the session.
