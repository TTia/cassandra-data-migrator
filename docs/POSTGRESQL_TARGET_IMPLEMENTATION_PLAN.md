# Cassandra to PostgreSQL Migration: Implementation Plan

## Executive Summary

This document outlines the implementation plan for extending the Cassandra Data Migrator (CDM) to support PostgreSQL as a target database. The existing architecture is well-designed with clear separation of concerns, making this extension feasible through implementation of new target-side components while reusing the proven Cassandra source reading pipeline.

### Feasibility Assessment: **VIABLE**

| Aspect | Assessment | Notes |
|--------|------------|-------|
| Architecture Extensibility | **Excellent** | Clear interfaces and factory patterns |
| Source Pipeline Reuse | **100%** | No changes needed to Cassandra read path |
| Type System Mapping | **100% Coverage** | All used types have direct PostgreSQL equivalents |
| Batch Execution | **Adaptable** | JDBC batch APIs are mature |
| Feature Compatibility | **Full** | All required features supported |

> **Note:** This plan excludes TTL and Counter support as these features are not used in the source Cassandra cluster.

---

## 1. Architecture Overview

### Current Architecture (Cassandra → Cassandra)

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           SPARK DRIVER                                   │
│  ┌─────────────┐    ┌──────────────────┐    ┌─────────────────────┐    │
│  │ Migrate.scala│───▶│PartitionRanges   │───▶│ Spark Parallelize   │    │
│  └─────────────┘    └──────────────────┘    └─────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────┘
                                │
                    ┌───────────┴───────────┐
                    ▼                       ▼
        ┌─────────────────────┐  ┌─────────────────────┐
        │   SPARK WORKER 1    │  │   SPARK WORKER N    │
        │ ┌─────────────────┐ │  │ ┌─────────────────┐ │
        │ │ CopyJobSession  │ │  │ │ CopyJobSession  │ │
        │ │                 │ │  │ │                 │ │
        │ │ ┌─────────────┐ │ │  │ │ ┌─────────────┐ │ │
        │ │ │Origin SELECT│ │ │  │ │ │Origin SELECT│ │ │
        │ │ │(Cassandra)  │ │ │  │ │ │(Cassandra)  │ │ │
        │ │ └──────┬──────┘ │ │  │ │ └──────┬──────┘ │ │
        │ │        ▼        │ │  │ │        ▼        │ │
        │ │ ┌─────────────┐ │ │  │ │ ┌─────────────┐ │ │
        │ │ │Record + PK  │ │ │  │ │ │Record + PK  │ │ │
        │ │ │Conversion   │ │ │  │ │ │Conversion   │ │ │
        │ │ └──────┬──────┘ │ │  │ │ └──────┬──────┘ │ │
        │ │        ▼        │ │  │ │        ▼        │ │
        │ │ ┌─────────────┐ │ │  │ │ ┌─────────────┐ │ │
        │ │ │Target UPSERT│ │ │  │ │ │Target UPSERT│ │ │
        │ │ │(Cassandra)  │ │ │  │ │ │(Cassandra)  │ │ │
        │ │ └─────────────┘ │ │  │ │ └─────────────┘ │ │
        │ └─────────────────┘ │  │ └─────────────────┘ │
        └─────────────────────┘  └─────────────────────┘
```

### Proposed Architecture (Cassandra → PostgreSQL)

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           SPARK DRIVER                                   │
│  ┌─────────────┐    ┌──────────────────┐    ┌─────────────────────┐    │
│  │ Migrate.scala│───▶│PartitionRanges   │───▶│ Spark Parallelize   │    │
│  └─────────────┘    └──────────────────┘    └─────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────┘
                                │
                    ┌───────────┴───────────┐
                    ▼                       ▼
        ┌─────────────────────┐  ┌─────────────────────┐
        │   SPARK WORKER 1    │  │   SPARK WORKER N    │
        │ ┌─────────────────┐ │  │ ┌─────────────────┐ │
        │ │PostgresJob     │ │  │ │PostgresJob      │ │
        │ │Session (NEW)   │ │  │ │Session (NEW)    │ │
        │ │                 │ │  │ │                 │ │
        │ │ ┌─────────────┐ │ │  │ │ ┌─────────────┐ │ │
        │ │ │Origin SELECT│ │ │  │ │ │Origin SELECT│ │ │
        │ │ │(Cassandra)  │ │ │  │ │ │(Cassandra)  │ │ │
        │ │ │ [UNCHANGED] │ │ │  │ │ │ [UNCHANGED] │ │ │
        │ │ └──────┬──────┘ │ │  │ │ └──────┬──────┘ │ │
        │ │        ▼        │ │  │ │        ▼        │ │
        │ │ ┌─────────────┐ │ │  │ │ ┌─────────────┐ │ │
        │ │ │Record + PG  │ │ │  │ │ │Record + PG  │ │ │
        │ │ │Type Convert │ │ │  │ │ │Type Convert │ │ │
        │ │ │   [NEW]     │ │ │  │ │ │   [NEW]     │ │ │
        │ │ └──────┬──────┘ │ │  │ │ └──────┬──────┘ │ │
        │ │        ▼        │ │  │ │        ▼        │ │
        │ │ ┌─────────────┐ │ │  │ │ ┌─────────────┐ │ │
        │ │ │PG UPSERT    │ │ │  │ │ │PG UPSERT    │ │ │
        │ │ │(JDBC Batch) │ │ │  │ │ │(JDBC Batch) │ │ │
        │ │ │   [NEW]     │ │ │  │ │ │   [NEW]     │ │ │
        │ │ └─────────────┘ │ │  │ │ └─────────────┘ │ │
        │ └─────────────────┘ │  │ └─────────────────┘ │
        └─────────────────────┘  └─────────────────────┘
```

---

## 2. Component Design

### 2.1 New Package Structure

```
src/main/java/com/datastax/cdm/
├── job/
│   ├── PostgresCopyJobSession.java      [NEW] - PG-specific job session
│   └── PostgresCopyJobSessionFactory.java [NEW] - Factory for PG sessions
├── cql/
│   └── statement/
│       └── PostgresUpsertStatement.java  [NEW] - PG INSERT/UPSERT builder
├── schema/
│   ├── PostgresTable.java               [NEW] - PG schema metadata
│   └── PostgresTypeMapper.java          [NEW] - Cassandra→PG type mapping
├── connect/
│   └── PostgresConnectionFactory.java   [NEW] - JDBC connection pooling
└── properties/
    └── KnownProperties.java             [MODIFIED] - Add PG properties
```

### 2.2 Component Specifications

#### 2.2.1 PostgresConnectionFactory

**Purpose:** Manage JDBC connection pooling for PostgreSQL target

**Location:** `src/main/java/com/datastax/cdm/connect/PostgresConnectionFactory.java`

```java
public class PostgresConnectionFactory {
    private HikariDataSource dataSource;

    // Configuration from properties
    private String jdbcUrl;      // spark.cdm.connect.target.postgres.url
    private String username;     // spark.cdm.connect.target.postgres.username
    private String password;     // spark.cdm.connect.target.postgres.password
    private int maxPoolSize;     // spark.cdm.connect.target.postgres.pool.size

    public Connection getConnection();
    public void close();

    // Connection pool statistics for monitoring
    public PoolStats getPoolStats();
}
```

**Dependencies:**
- HikariCP (connection pooling)
- PostgreSQL JDBC Driver

#### 2.2.2 PostgresTable

**Purpose:** Discover and hold PostgreSQL table schema metadata

**Location:** `src/main/java/com/datastax/cdm/schema/PostgresTable.java`

```java
public class PostgresTable extends BaseTable {

    // Schema discovery from information_schema
    public void loadMetadata(Connection connection);

    // Column metadata
    public List<String> getColumnNames();
    public List<PostgresType> getColumnTypes();
    public List<String> getPrimaryKeyColumns();

    // Type binding
    public int getSqlType(int columnIndex);      // java.sql.Types constant
    public Class<?> getJavaType(int columnIndex);

    // Constraint information
    public List<String> getUniqueConstraints();
    public boolean hasOnConflictSupport();       // PG 9.5+
}
```

**Schema Discovery Query:**
```sql
SELECT
    c.column_name,
    c.data_type,
    c.udt_name,
    c.is_nullable,
    c.column_default,
    tc.constraint_type
FROM information_schema.columns c
LEFT JOIN information_schema.key_column_usage kcu
    ON c.column_name = kcu.column_name
    AND c.table_name = kcu.table_name
LEFT JOIN information_schema.table_constraints tc
    ON kcu.constraint_name = tc.constraint_name
WHERE c.table_schema = ? AND c.table_name = ?
ORDER BY c.ordinal_position;
```

#### 2.2.3 PostgresTypeMapper

**Purpose:** Map Cassandra types to PostgreSQL types and handle conversions

**Location:** `src/main/java/com/datastax/cdm/schema/PostgresTypeMapper.java`

```java
public class PostgresTypeMapper {

    // Primary mapping method
    public PostgresType mapCassandraType(DataType cassandraType);

    // Value conversion
    public Object convertValue(Object cassandraValue, DataType fromType, PostgresType toType);

    // Collection handling
    public Array convertList(List<?> list, Connection conn, PostgresType elementType);
    public Array convertSet(Set<?> set, Connection conn, PostgresType elementType);
    public PGobject convertMap(Map<?,?> map);        // To JSONB
    public PGobject convertUDT(UdtValue udt);        // To JSONB

    // Special type handlers
    public Object convertDuration(CqlDuration duration);
    public Object convertInet(InetAddress inet);
    public Object convertBlob(ByteBuffer blob);
}
```

**Complete Type Mapping Table:**

| Cassandra Type | PostgreSQL Type | SQL Type Constant | Java Class |
|----------------|-----------------|-------------------|------------|
| TEXT | TEXT | Types.VARCHAR | String |
| ASCII | TEXT | Types.VARCHAR | String |
| INT | INTEGER | Types.INTEGER | Integer |
| BIGINT | BIGINT | Types.BIGINT | Long |
| SMALLINT | SMALLINT | Types.SMALLINT | Short |
| TINYINT | SMALLINT | Types.SMALLINT | Short |
| FLOAT | REAL | Types.REAL | Float |
| DOUBLE | DOUBLE PRECISION | Types.DOUBLE | Double |
| DECIMAL | NUMERIC | Types.NUMERIC | BigDecimal |
| VARINT | NUMERIC | Types.NUMERIC | BigDecimal |
| BOOLEAN | BOOLEAN | Types.BOOLEAN | Boolean |
| UUID | UUID | Types.OTHER | UUID |
| TIMEUUID | UUID | Types.OTHER | UUID |
| TIMESTAMP | TIMESTAMP WITH TIME ZONE | Types.TIMESTAMP_WITH_TIMEZONE | OffsetDateTime |
| DATE | DATE | Types.DATE | LocalDate |
| TIME | TIME | Types.TIME | LocalTime |
| DURATION | INTERVAL | Types.OTHER | PGInterval |
| BLOB | BYTEA | Types.BINARY | byte[] |
| INET | INET | Types.OTHER | PGobject |
| LIST<T> | ARRAY[T] | Types.ARRAY | Array |
| SET<T> | ARRAY[T] | Types.ARRAY | Array |
| MAP<K,V> | JSONB | Types.OTHER | PGobject |
| UDT | JSONB | Types.OTHER | PGobject |
| TUPLE | JSONB | Types.OTHER | PGobject |
| VECTOR<FLOAT> | vector (pgvector) | Types.OTHER | PGobject |

#### 2.2.4 PostgresUpsertStatement

**Purpose:** Build and execute PostgreSQL INSERT/UPSERT statements

**Location:** `src/main/java/com/datastax/cdm/cql/statement/PostgresUpsertStatement.java`

```java
public class PostgresUpsertStatement {

    private final PostgresTable table;
    private final PostgresTypeMapper typeMapper;
    private PreparedStatement preparedStatement;

    // Statement building
    public String buildInsertStatement();
    public String buildUpsertStatement();  // INSERT ... ON CONFLICT DO UPDATE

    // Binding from Cassandra Row
    public void bindRecord(Record record, PreparedStatement stmt);

    // Batch execution
    public void addToBatch(Record record);
    public int[] executeBatch();
    public void clearBatch();

    // Transaction control
    public void setAutoCommit(boolean autoCommit);
    public void commit();
    public void rollback();
}
```

**Generated SQL Patterns:**

```sql
-- Standard INSERT (for new tables)
INSERT INTO schema.table (col1, col2, col3, ...)
VALUES (?, ?, ?, ...)

-- UPSERT with ON CONFLICT (PG 9.5+)
INSERT INTO schema.table (col1, col2, col3, ...)
VALUES (?, ?, ?, ...)
ON CONFLICT (pk1, pk2) DO UPDATE SET
    col3 = EXCLUDED.col3,
    col4 = EXCLUDED.col4,
    ...
```

#### 2.2.5 PostgresCopyJobSession

**Purpose:** Orchestrate the migration process from Cassandra to PostgreSQL

**Location:** `src/main/java/com/datastax/cdm/job/PostgresCopyJobSession.java`

```java
public class PostgresCopyJobSession extends AbstractJobSession<PartitionRange> {

    // PostgreSQL-specific components
    private final PostgresConnectionFactory connectionFactory;
    private final PostgresTable targetTable;
    private final PostgresUpsertStatement upsertStatement;
    private final PostgresTypeMapper typeMapper;

    // Batch management
    private int batchSize;
    private int currentBatchCount;

    @Override
    public void processPartitionRange(PartitionRange range) {
        // 1. Execute Cassandra SELECT (reuse existing)
        ResultSet resultSet = originSelectByPartitionRangeStatement.execute(range);

        // 2. Process each row
        for (Row originRow : resultSet) {
            rateLimiterOrigin.acquire(1);
            jobCounter.increment(CounterType.READ);

            Record record = new Record(pkFactory.getTargetPK(originRow), originRow, null);

            // 3. Apply transformations (reuse existing features)
            for (Record r : pkFactory.toValidRecordList(record)) {

                // 4. Bind to PostgreSQL prepared statement
                upsertStatement.addToBatch(r);
                currentBatchCount++;

                // 5. Execute batch when threshold reached
                if (currentBatchCount >= batchSize) {
                    executeBatchWithRetry();
                    currentBatchCount = 0;
                }
            }
        }

        // 6. Flush remaining batch
        if (currentBatchCount > 0) {
            executeBatchWithRetry();
        }
    }

    private void executeBatchWithRetry() {
        // Retry logic with exponential backoff
    }
}
```

#### 2.2.6 PostgresCopyJobSessionFactory

**Purpose:** Factory for creating PostgresCopyJobSession instances

**Location:** `src/main/java/com/datastax/cdm/job/PostgresCopyJobSessionFactory.java`

```java
public class PostgresCopyJobSessionFactory implements IJobSessionFactory<PartitionRange> {

    @Override
    public AbstractJobSession<PartitionRange> getInstance(
            CqlSession originSession,
            CqlSession targetSession,  // Will be null for PG target
            PropertyHelper propHelper) {

        // Create PostgreSQL connection factory
        PostgresConnectionFactory pgConnFactory = new PostgresConnectionFactory(propHelper);

        // Create and return session
        return new PostgresCopyJobSession(originSession, pgConnFactory, propHelper);
    }
}
```

---

## 3. Configuration Properties

### 3.1 New PostgreSQL Target Properties

Add to `KnownProperties.java`:

```java
// PostgreSQL Connection Properties
public static final String TARGET_TYPE = "spark.cdm.connect.target.type";  // "cassandra" or "postgres"
public static final String PG_JDBC_URL = "spark.cdm.connect.target.postgres.url";
public static final String PG_USERNAME = "spark.cdm.connect.target.postgres.username";
public static final String PG_PASSWORD = "spark.cdm.connect.target.postgres.password";
public static final String PG_SCHEMA = "spark.cdm.connect.target.postgres.schema";
public static final String PG_TABLE = "spark.cdm.connect.target.postgres.table";

// PostgreSQL Pool Configuration
public static final String PG_POOL_SIZE = "spark.cdm.connect.target.postgres.pool.size";
public static final String PG_POOL_TIMEOUT = "spark.cdm.connect.target.postgres.pool.timeout";

// PostgreSQL Write Configuration
public static final String PG_BATCH_SIZE = "spark.cdm.connect.target.postgres.batchSize";
public static final String PG_UPSERT_MODE = "spark.cdm.connect.target.postgres.upsertMode";  // insert/upsert
public static final String PG_ON_CONFLICT_COLUMNS = "spark.cdm.connect.target.postgres.onConflictColumns";

// PostgreSQL Transaction Configuration
public static final String PG_TRANSACTION_SIZE = "spark.cdm.connect.target.postgres.transactionSize";
public static final String PG_ISOLATION_LEVEL = "spark.cdm.connect.target.postgres.isolationLevel";

// PostgreSQL Type Handling
public static final String PG_MAP_TO_JSONB = "spark.cdm.connect.target.postgres.mapToJsonb";  // true/false
public static final String PG_UDT_TO_JSONB = "spark.cdm.connect.target.postgres.udtToJsonb";  // true/false
public static final String PG_ARRAY_TYPE_PREFIX = "spark.cdm.connect.target.postgres.array.";
```

### 3.2 Example Configuration File

```properties
# cdm-postgres.properties

# Origin: Cassandra
spark.cdm.connect.origin.host=cassandra-cluster.example.com
spark.cdm.connect.origin.port=9042
spark.cdm.connect.origin.username=cassandra_user
spark.cdm.connect.origin.password=cassandra_pass
spark.cdm.schema.origin.keyspaceTable=my_keyspace.my_table

# Target Type Selection
spark.cdm.connect.target.type=postgres

# Target: PostgreSQL
spark.cdm.connect.target.postgres.url=jdbc:postgresql://pg-host:5432/mydb
spark.cdm.connect.target.postgres.username=pg_user
spark.cdm.connect.target.postgres.password=pg_pass
spark.cdm.connect.target.postgres.schema=public
spark.cdm.connect.target.postgres.table=my_table

# PostgreSQL Pool Settings
spark.cdm.connect.target.postgres.pool.size=10
spark.cdm.connect.target.postgres.pool.timeout=30000

# PostgreSQL Write Settings
spark.cdm.connect.target.postgres.batchSize=1000
spark.cdm.connect.target.postgres.upsertMode=upsert
spark.cdm.connect.target.postgres.onConflictColumns=id,partition_key

# Transaction Settings
spark.cdm.connect.target.postgres.transactionSize=5000
spark.cdm.connect.target.postgres.isolationLevel=READ_COMMITTED

# Type Mapping Options
spark.cdm.connect.target.postgres.mapToJsonb=true
spark.cdm.connect.target.postgres.udtToJsonb=true

# Performance
spark.cdm.perfops.numParts=5000
spark.cdm.perfops.ratelimit.origin=20000
spark.cdm.perfops.ratelimit.target=50000
```

---

## 4. Implementation Phases

### Phase 0: CI/CD Pipeline Setup (Pre-requisite)

**Deliverables:**
1. GitHub Actions workflow for PostgreSQL target CI/CD
2. Build and unit test automation across JDK 11, 17, 21
3. Integration tests with TimescaleDB (via GitHub Services)
4. Testcontainers-based integration tests
5. Code quality checks

**Files Created:**
```
.github/workflows/
└── postgres-target-ci.yml                 [CREATE] - PostgreSQL target CI pipeline
```

**Workflow Features:**
- **Path-based triggers:** Only runs on changes to PostgreSQL-related files
- **Matrix builds:** Tests across JDK 11, 17, 21
- **TimescaleDB service:** Spins up `timescale/timescaledb:latest-pg16` for integration tests
- **Testcontainers support:** For tests using `@Testcontainers` annotation
- **Artifact upload:** Test results preserved for debugging

**Acceptance Criteria:**
- [x] Workflow triggers on relevant file changes
- [x] Unit tests run across multiple JDK versions
- [x] Integration tests connect to TimescaleDB service
- [x] Testcontainers tests can pull and use TimescaleDB image
- [x] All test results uploaded as artifacts

---

### Phase 1: Core Infrastructure (Weeks 1-2)

**Deliverables:**
1. Maven dependencies (PostgreSQL JDBC, HikariCP)
2. PostgresConnectionFactory with connection pooling
3. PostgresTable with schema discovery
4. Basic PostgresTypeMapper for primitive types
5. Unit tests for connection and type mapping

**Files to Create/Modify:**
```
pom.xml                                    [MODIFY] - Add dependencies
src/main/java/com/datastax/cdm/
├── connect/
│   └── PostgresConnectionFactory.java     [CREATE]
├── schema/
│   ├── PostgresTable.java                 [CREATE]
│   └── PostgresTypeMapper.java            [CREATE]
└── properties/
    └── KnownProperties.java               [MODIFY]
```

**Acceptance Criteria:**
- [x] Can connect to PostgreSQL with connection pooling
- [x] Can discover table schema from information_schema
- [x] Can map all primitive Cassandra types to PostgreSQL
- [x] Unit tests pass with >80% coverage

### Phase 2: Statement Building & Execution (Weeks 3-4)

**Deliverables:**
1. PostgresUpsertStatement with INSERT and UPSERT support
2. Batch execution with configurable batch sizes
3. Transaction management
4. Error handling and retry logic

**Files to Create:**
```
src/main/java/com/datastax/cdm/cql/statement/
└── PostgresUpsertStatement.java           [CREATE]
```

**Acceptance Criteria:**
- [x] Can generate correct INSERT statements
- [x] Can generate correct UPSERT (ON CONFLICT) statements
- [x] Batch execution works with configurable sizes
- [x] Transactions commit/rollback correctly
- [x] Retry logic handles transient failures

### Phase 3: Job Session Integration (Weeks 5-6)

**Deliverables:**
1. PostgresCopyJobSession
2. PostgresCopyJobSessionFactory
3. Integration with existing feature system
4. Rate limiting for PostgreSQL writes

**Files to Create/Modify:**
```
src/main/java/com/datastax/cdm/job/
├── PostgresCopyJobSession.java            [CREATE]
├── PostgresCopyJobSessionFactory.java     [CREATE]
└── AbstractJobSession.java                [MODIFY] - Add target type detection
src/main/scala/com/datastax/cdm/job/
└── Migrate.scala                          [MODIFY] - Support PG target
```

**Acceptance Criteria:**
- [ ] Can migrate data from Cassandra to PostgreSQL
- [ ] Features work (ConstantColumns, ExplodeMap, ExtractJson)
- [ ] Rate limiting applies to PostgreSQL writes
- [ ] Progress tracking works correctly

### Phase 4: Complex Type Support (Week 7)

**Deliverables:**
1. Collection type support (LIST → ARRAY, SET → ARRAY)
2. MAP → JSONB conversion
3. UDT → JSONB conversion

**Files to Modify:**
```
src/main/java/com/datastax/cdm/schema/
└── PostgresTypeMapper.java                [MODIFY] - Add complex types
src/main/java/com/datastax/cdm/cql/statement/
└── PostgresUpsertStatement.java           [MODIFY] - Handle complex binding
```

**Acceptance Criteria:**
- [ ] LIST columns migrate to PostgreSQL ARRAY
- [ ] SET columns migrate to PostgreSQL ARRAY (deduplicated)
- [ ] MAP columns migrate to JSONB
- [ ] UDT columns migrate to JSONB

### Phase 5: Validation & Diff Support (Weeks 8-9)

**Deliverables:**
1. PostgresDiffJobSession for data validation
2. PostgresSelectByPKStatement for lookups
3. Diff reporting for Cassandra vs PostgreSQL comparison

**Files to Create:**
```
src/main/java/com/datastax/cdm/job/
├── PostgresDiffJobSession.java            [CREATE]
└── PostgresDiffJobSessionFactory.java     [CREATE]
src/main/java/com/datastax/cdm/cql/statement/
└── PostgresSelectByPKStatement.java       [CREATE]
```

**Acceptance Criteria:**
- [ ] Can validate migrated data matches source
- [ ] Diff reports show missing/different records
- [ ] Supports primary key lookups on PostgreSQL

### Phase 6: Testing & Documentation (Week 10)

**Deliverables:**
1. Integration tests with embedded PostgreSQL
2. Performance benchmarks
3. User documentation
4. Configuration examples

**Files to Create:**
```
src/test/java/com/datastax/cdm/
├── job/PostgresCopyJobSessionTest.java    [CREATE]
├── schema/PostgresTypeMapperTest.java     [CREATE]
└── integration/
    └── CassandraToPostgresTest.java       [CREATE]
src/resources/
├── cdm-postgres.properties                [CREATE]
└── cdm-postgres-detailed.properties       [CREATE]
docs/
├── POSTGRESQL_TARGET.md                   [CREATE]
└── TYPE_MAPPING_REFERENCE.md              [CREATE]
```

**Acceptance Criteria:**
- [ ] All integration tests pass
- [ ] Performance meets baseline targets
- [ ] Documentation complete and reviewed
- [ ] Example configurations provided

---

## 5. Dependencies

### 5.1 Maven Dependencies to Add

```xml
<!-- PostgreSQL JDBC Driver -->
<dependency>
    <groupId>org.postgresql</groupId>
    <artifactId>postgresql</artifactId>
    <version>42.7.1</version>
</dependency>

<!-- HikariCP Connection Pooling -->
<dependency>
    <groupId>com.zaxxer</groupId>
    <artifactId>HikariCP</artifactId>
    <version>5.1.0</version>
</dependency>

<!-- For JSONB handling -->
<dependency>
    <groupId>com.google.code.gson</groupId>
    <artifactId>gson</artifactId>
    <version>2.10.1</version>
</dependency>

<!-- Optional: pgvector support for VECTOR type -->
<dependency>
    <groupId>com.pgvector</groupId>
    <artifactId>pgvector</artifactId>
    <version>0.1.4</version>
    <optional>true</optional>
</dependency>

<!-- Testing: Testcontainers with TimescaleDB -->
<dependency>
    <groupId>org.testcontainers</groupId>
    <artifactId>testcontainers</artifactId>
    <version>1.19.3</version>
    <scope>test</scope>
</dependency>
<dependency>
    <groupId>org.testcontainers</groupId>
    <artifactId>postgresql</artifactId>
    <version>1.19.3</version>
    <scope>test</scope>
</dependency>
<dependency>
    <groupId>org.testcontainers</groupId>
    <artifactId>junit-jupiter</artifactId>
    <version>1.19.3</version>
    <scope>test</scope>
</dependency>
```

**TimescaleDB Docker Image:** `timescale/timescaledb:latest-pg16`

---

## 6. Risk Assessment & Mitigations

### 6.1 Technical Risks

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| Type conversion data loss | Medium | High | Comprehensive testing, validation mode, logging warnings |
| Performance degradation | Medium | Medium | Batch tuning, connection pooling, benchmarking |
| Large collection handling | Medium | Medium | JSONB for very large collections, configurable thresholds |
| Network/connection failures | Medium | Low | Retry logic with exponential backoff, connection pooling |

### 6.2 Feature Compatibility Matrix

| Feature | Cassandra Target | PostgreSQL Target | Notes |
|---------|------------------|-------------------|-------|
| Basic migration | Full | Full | Core functionality |
| ConstantColumns | Full | Full | No changes needed |
| ExplodeMap | Full | Full | Works with JSONB or separate columns |
| ExtractJson | Full | Full | No changes needed |
| OriginFilter | Full | Full | No changes needed |
| Validation/Diff | Full | Full | New implementation needed |
| Track/Resume | Full | Full | Uses same tracking mechanism |

> **Excluded Features:** TTL and Counter tables are not supported as they are not used in the source cluster.

---

## 7. Testing Strategy

### 7.1 Unit Tests

- PostgresConnectionFactory connection management
- PostgresTable schema discovery
- PostgresTypeMapper for all type conversions
- PostgresUpsertStatement SQL generation
- Batch execution logic

### 7.2 Integration Tests

Using Testcontainers with TimescaleDB:

```java
@Testcontainers
class CassandraToPostgresIntegrationTest {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(
            DockerImageName.parse("timescale/timescaledb:latest-pg16")
                    .asCompatibleSubstituteFor("postgres"))
            .withDatabaseName("testdb")
            .withUsername("test")
            .withPassword("test");

    @Test
    void testPrimitiveMigration() {
        // Migrate INT, TEXT, TIMESTAMP, UUID columns
    }

    @Test
    void testCollectionMigration() {
        // Migrate LIST, SET, MAP columns
    }

    @Test
    void testUDTMigration() {
        // Migrate User-Defined Types to JSONB
    }

    @Test
    void testUpsertConflictResolution() {
        // Test ON CONFLICT DO UPDATE behavior
    }

    @Test
    void testBatchExecution() {
        // Test various batch sizes and transaction boundaries
    }

    @Test
    void testLargeDatasetMigration() {
        // Performance test with 1M+ records
    }
}
```

### 7.3 Performance Benchmarks

| Metric | Target | Test Approach |
|--------|--------|---------------|
| Throughput | >10K rows/sec | Sustained load test |
| Latency (p99) | <100ms per batch | Monitor batch execution times |
| Memory usage | <2GB per worker | Profile under load |
| Connection pool efficiency | >95% utilization | Monitor pool stats |

---

## 8. Success Criteria

### 8.1 Functional Requirements

1. **Complete Type Support:** All Cassandra types migrate correctly to PostgreSQL
2. **Data Integrity:** Zero data loss for supported types
3. **Upsert Support:** ON CONFLICT handling for idempotent migrations
4. **Batch Efficiency:** Configurable batch sizes with transaction control
5. **Feature Compatibility:** Existing features work with PostgreSQL target
6. **Validation:** Diff mode validates migrated data

### 8.2 Non-Functional Requirements

1. **Performance:** Within 20% of Cassandra-to-Cassandra throughput
2. **Scalability:** Scales linearly with Spark workers
3. **Reliability:** Graceful handling of transient failures
4. **Observability:** Comprehensive logging and metrics
5. **Documentation:** Complete user and developer documentation

---

## 9. Appendix

### 9.1 PostgreSQL Schema Example

For a Cassandra table:

```cql
CREATE TABLE my_keyspace.users (
    user_id UUID,
    email TEXT,
    created_at TIMESTAMP,
    profile MAP<TEXT, TEXT>,
    tags SET<TEXT>,
    login_history LIST<TIMESTAMP>,
    address FROZEN<address_udt>,
    PRIMARY KEY (user_id)
);
```

Equivalent PostgreSQL table:

```sql
CREATE TABLE public.users (
    user_id UUID PRIMARY KEY,
    email TEXT,
    created_at TIMESTAMP WITH TIME ZONE,
    profile JSONB,
    tags TEXT[],
    login_history TIMESTAMP WITH TIME ZONE[],
    address JSONB
);
```

### 9.2 Error Handling Strategy

```java
public enum PostgresErrorCode {
    UNIQUE_VIOLATION("23505"),      // Constraint violation - may retry with upsert
    FOREIGN_KEY_VIOLATION("23503"), // Skip or log
    NOT_NULL_VIOLATION("23502"),    // Log and skip record
    CONNECTION_ERROR("08xxx"),       // Retry with backoff
    TRANSACTION_ROLLBACK("40xxx"),   // Retry entire batch
    DISK_FULL("53100");              // Fatal - stop migration

    // Retry policy based on error code
    public RetryPolicy getRetryPolicy();
}
```

### 9.3 Monitoring Metrics

```
cdm.postgres.connection.active     - Active connections in pool
cdm.postgres.connection.pending    - Pending connection requests
cdm.postgres.batch.size            - Records per batch
cdm.postgres.batch.latency         - Batch execution time (ms)
cdm.postgres.records.migrated      - Total records migrated
cdm.postgres.records.failed        - Failed record count
cdm.postgres.type.conversion.errors - Type conversion failures
```

---

## 10. Conclusion

The Cassandra Data Migrator can be effectively extended to support PostgreSQL as a target database. The existing architecture provides excellent extension points through its factory patterns, abstract base classes, and clear separation between source reading and target writing.

**Key Advantages:**
- Reuse 100% of the Cassandra source reading infrastructure
- Reuse existing transformation features (ConstantColumns, ExplodeMap, ExtractJson)
- Reuse Spark-based distribution and parallelization
- Reuse rate limiting and progress tracking
- No TTL or Counter complexity (not used in source cluster)

**Key Challenges:**
- Complex types (MAP, UDT) require JSONB conversion (well-supported approach)
- Collection types require PostgreSQL ARRAY handling

**Estimated Effort:** 8-9 weeks for complete implementation with testing and documentation.

**Recommended Approach:** Start with Phase 1 (Core Infrastructure) to validate the architecture, then proceed incrementally through subsequent phases with regular testing and feedback cycles.
