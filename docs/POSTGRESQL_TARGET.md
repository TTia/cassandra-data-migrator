# PostgreSQL Target Support

The Cassandra Data Migrator (CDM) supports PostgreSQL as a target database, enabling migration from Cassandra to PostgreSQL/TimescaleDB.

## Table of Contents

- [Overview](#overview)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [Type Mapping](#type-mapping)
- [Performance Tuning](#performance-tuning)
- [Validation Mode](#validation-mode)
- [Troubleshooting](#troubleshooting)

## Overview

PostgreSQL target support allows you to:
- Migrate data from Cassandra to PostgreSQL
- Preserve data types with automatic type mapping
- Handle complex types (collections, UDTs) via JSONB
- Validate migrated data with diff mode
- Auto-correct discrepancies

### Supported PostgreSQL Versions
- PostgreSQL 9.5+ (for ON CONFLICT support)
- TimescaleDB (all versions)

## Quick Start

### 1. Prepare PostgreSQL Table

Create your target table in PostgreSQL with appropriate column types:

```sql
CREATE TABLE target_table (
    id UUID PRIMARY KEY,
    name TEXT,
    created_at TIMESTAMP WITH TIME ZONE,
    tags TEXT[],
    metadata JSONB
);
```

### 2. Configure CDM

Create a properties file (`cdm-postgres.properties`):

```properties
# Origin Cassandra
spark.cdm.connect.origin.host=cassandra.example.com
spark.cdm.connect.origin.port=9042
spark.cdm.schema.origin.keyspaceTable=keyspace.source_table

# Target PostgreSQL
spark.cdm.connect.target.type=postgres
spark.cdm.connect.target.postgres.url=jdbc:postgresql://localhost:5432/mydb
spark.cdm.connect.target.postgres.username=postgres
spark.cdm.connect.target.postgres.password=secret
spark.cdm.connect.target.postgres.schema=public
spark.cdm.connect.target.postgres.table=target_table
```

### 3. Run Migration

```bash
spark-submit \
  --class com.datastax.cdm.job.Migrate \
  --master "local[*]" \
  cassandra-data-migrator.jar \
  cdm-postgres.properties
```

## Configuration

### Connection Settings

| Property | Description | Default |
|----------|-------------|---------|
| `spark.cdm.connect.target.type` | Target database type | `cassandra` |
| `spark.cdm.connect.target.postgres.url` | JDBC connection URL | (required) |
| `spark.cdm.connect.target.postgres.username` | Database username | (required) |
| `spark.cdm.connect.target.postgres.password` | Database password | (required) |
| `spark.cdm.connect.target.postgres.schema` | Target schema | `public` |
| `spark.cdm.connect.target.postgres.table` | Target table name | (origin table) |

### Connection Pool

| Property | Description | Default |
|----------|-------------|---------|
| `spark.cdm.connect.target.postgres.pool.size` | Max pool connections | `10` |
| `spark.cdm.connect.target.postgres.pool.timeout` | Connection timeout (ms) | `30000` |

### Write Settings

| Property | Description | Default |
|----------|-------------|---------|
| `spark.cdm.connect.target.postgres.batchSize` | Records per JDBC batch | `1000` |
| `spark.cdm.connect.target.postgres.upsertMode` | `insert` or `upsert` | `upsert` |
| `spark.cdm.connect.target.postgres.onConflictColumns` | Columns for ON CONFLICT | (primary key) |
| `spark.cdm.connect.target.postgres.transactionSize` | Records per transaction | `5000` |
| `spark.cdm.connect.target.postgres.isolationLevel` | Transaction isolation | `READ_COMMITTED` |

### Type Handling

| Property | Description | Default |
|----------|-------------|---------|
| `spark.cdm.connect.target.postgres.mapToJsonb` | Convert MAP to JSONB | `true` |
| `spark.cdm.connect.target.postgres.udtToJsonb` | Convert UDT to JSONB | `true` |

## Type Mapping

### Primitive Types

| Cassandra Type | PostgreSQL Type | Notes |
|----------------|-----------------|-------|
| TEXT, ASCII | TEXT | |
| INT | INTEGER | |
| BIGINT | BIGINT | |
| SMALLINT | SMALLINT | |
| TINYINT | SMALLINT | PostgreSQL has no TINYINT |
| FLOAT | REAL | |
| DOUBLE | DOUBLE PRECISION | |
| DECIMAL, VARINT | NUMERIC | |
| BOOLEAN | BOOLEAN | |
| UUID, TIMEUUID | UUID | |
| TIMESTAMP | TIMESTAMP WITH TIME ZONE | Converted to UTC |
| DATE | DATE | |
| TIME | TIME | |
| DURATION | INTERVAL | |
| BLOB | BYTEA | |
| INET | INET | |

### Collection Types

| Cassandra Type | PostgreSQL Type | Notes |
|----------------|-----------------|-------|
| LIST\<T\> | T[] (ARRAY) | Native PostgreSQL arrays |
| SET\<T\> | T[] (ARRAY) | Duplicates removed |
| MAP\<K,V\> | JSONB | Key-value pairs as JSON |

### Complex Types

| Cassandra Type | PostgreSQL Type | Notes |
|----------------|-----------------|-------|
| UDT | JSONB | Field names preserved |
| TUPLE | JSONB | Stored as JSON array |
| VECTOR | vector (pgvector) | Requires pgvector extension |

### JSON Structure Examples

**MAP to JSONB:**
```json
// Cassandra: MAP<TEXT, INT>
{"key1": 100, "key2": 200}
```

**UDT to JSONB:**
```json
// Cassandra: address UDT with street, city, zip
{"street": "123 Main St", "city": "Boston", "zip": "02101"}
```

**TUPLE to JSONB:**
```json
// Cassandra: TUPLE<TEXT, INT, BOOLEAN>
["value", 42, true]
```

## Performance Tuning

### Batch Size

Larger batches improve throughput but use more memory:

```properties
# For high-throughput workloads
spark.cdm.connect.target.postgres.batchSize=2000

# For memory-constrained environments
spark.cdm.connect.target.postgres.batchSize=500
```

### Connection Pool

Size your pool based on Spark parallelism:

```properties
# Rule: pool_size >= num_executors * executor_cores
spark.cdm.connect.target.postgres.pool.size=20
```

### Rate Limiting

Control migration speed to avoid overwhelming PostgreSQL:

```properties
# Limit writes to 50,000 records/second
spark.cdm.perfops.ratelimit.target=50000
```

### Transaction Size

Balance between performance and recovery:

```properties
# Larger transactions = better performance
spark.cdm.connect.target.postgres.transactionSize=10000

# Smaller transactions = faster rollback on failure
spark.cdm.connect.target.postgres.transactionSize=1000
```

## Validation Mode

Run validation to compare Cassandra and PostgreSQL data:

```bash
spark-submit \
  --class com.datastax.cdm.job.DiffData \
  cassandra-data-migrator.jar \
  cdm-postgres.properties
```

### Auto-Correction

Enable auto-correction to fix discrepancies:

```properties
# Insert missing records
spark.cdm.autocorrect.missing=true

# Update mismatched records
spark.cdm.autocorrect.mismatch=true
```

## Troubleshooting

### Connection Issues

**Error:** `Connection refused`
- Verify PostgreSQL is running and accepting connections
- Check firewall rules
- Verify JDBC URL format

**Error:** `FATAL: password authentication failed`
- Verify username/password
- Check `pg_hba.conf` authentication settings

### Type Conversion Errors

**Error:** `Cannot convert type X to Y`
- Ensure target column types match expected PostgreSQL types
- Check type mapping table above
- For complex types, verify JSONB column type

### Performance Issues

**Slow writes:**
1. Increase batch size
2. Increase connection pool size
3. Check PostgreSQL `max_connections`
4. Disable indexes during migration (re-enable after)

**Memory errors:**
1. Reduce batch size
2. Reduce Spark executor memory
3. Increase transaction commit frequency

### Data Validation Failures

**Missing records:**
- Run with `spark.cdm.autocorrect.missing=true`
- Check for filtering conditions

**Mismatched records:**
- Verify type mapping is correct
- Check for timestamp timezone issues
- For numeric types, verify precision handling

## Best Practices

1. **Pre-create indexes after migration** - Disable indexes during migration for speed
2. **Monitor PostgreSQL connections** - Track connection pool usage
3. **Use appropriate batch sizes** - Start with 1000, adjust based on performance
4. **Test with sample data** - Validate type mapping before full migration
5. **Plan for rollback** - Keep transaction sizes manageable
6. **Monitor disk space** - JSONB columns can be larger than expected

## Example: Full Migration

```properties
# Complete production configuration
spark.cdm.connect.origin.host=cassandra-prod.example.com
spark.cdm.connect.origin.port=9042
spark.cdm.connect.origin.username=read_user
spark.cdm.connect.origin.password=secure_password
spark.cdm.connect.origin.localDC=us-east-1
spark.cdm.schema.origin.keyspaceTable=production.events

spark.cdm.connect.target.type=postgres
spark.cdm.connect.target.postgres.url=jdbc:postgresql://pg-prod.example.com:5432/analytics
spark.cdm.connect.target.postgres.username=migration_user
spark.cdm.connect.target.postgres.password=pg_secure_password
spark.cdm.connect.target.postgres.schema=public
spark.cdm.connect.target.postgres.table=events

spark.cdm.connect.target.postgres.pool.size=20
spark.cdm.connect.target.postgres.batchSize=1000
spark.cdm.connect.target.postgres.upsertMode=upsert
spark.cdm.connect.target.postgres.transactionSize=5000

spark.cdm.perfops.ratelimit.origin=50000
spark.cdm.perfops.ratelimit.target=100000
spark.cdm.perfops.numParts=10000
```
