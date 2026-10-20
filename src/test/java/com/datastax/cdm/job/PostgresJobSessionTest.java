/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.cdm.job;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.spark.SparkConf;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import com.datastax.cdm.connect.PostgresConnectionFactory;
import com.datastax.cdm.job.IJobSessionFactory.JobType;
import com.datastax.cdm.job.JobCounter.CounterType;
import com.datastax.cdm.properties.PropertyHelper;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;

/**
 * Runs the PostgreSQL copy and diff sessions end to end: a real Cassandra origin, a real PostgreSQL target. batchSize=2
 * and transactionSize=2, so a handful of rows already crosses batch and commit boundaries.
 */
@Testcontainers(disabledWithoutDocker = true)
public class PostgresJobSessionTest {

    private static final String TIMESCALE_IMAGE = "timescale/timescaledb:latest-pg16";
    private static final Instant DAY_START = Instant.parse("2026-09-01T00:00:00.123Z");
    private static final Instant OUT_OF_FILTER = Instant.parse("2026-09-10T00:00:00Z");
    private static final int ROWS_PER_DEVICE_IN_FILTER = 3;
    private static final String[] DEVICES = { "dev-1", "dev-2" };
    // 2^53 + 1: the nearest double is 2^53, so a double comparison cannot tell it from 2^53
    private static final long BIG = 9007199254740993L;

    @Container
    static GenericContainer<?> cassandra = new GenericContainer<>(DockerImageName.parse("cassandra:4.1"))
            .withExposedPorts(9042).withEnv("MAX_HEAP_SIZE", "512M").withEnv("HEAP_NEWSIZE", "128M")
            .waitingFor(Wait.forLogMessage(".*Starting listening for CQL clients.*\\n", 1)
                    .withStartupTimeout(Duration.ofMinutes(3)));

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(
            DockerImageName.parse(TIMESCALE_IMAGE).asCompatibleSubstituteFor("postgres")).withDatabaseName("testdb")
                    .withUsername("testuser").withPassword("testpass");

    private static CqlSession cql;
    private static PostgresConnectionFactory connectionFactory;

    @BeforeAll
    static void setup() throws SQLException {
        cql = CqlSession.builder()
                .addContactPoint(new InetSocketAddress(cassandra.getHost(), cassandra.getMappedPort(9042)))
                .withLocalDatacenter("datacenter1").build();
        cql.execute("CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
        cql.execute("CREATE TABLE ks.readings (device text, ts timestamp, value int, PRIMARY KEY (device, ts))");
        for (String device : DEVICES) {
            for (int i = 0; i < ROWS_PER_DEVICE_IN_FILTER; i++) {
                cql.execute(SimpleStatement.newInstance("INSERT INTO ks.readings (device, ts, value) VALUES (?, ?, ?)",
                        device, DAY_START.plus(Duration.ofHours(i)), i));
            }
            cql.execute(SimpleStatement.newInstance("INSERT INTO ks.readings (device, ts, value) VALUES (?, ?, ?)",
                    device, OUT_OF_FILTER, 99));
        }

        cql.execute("CREATE TYPE ks.period (starts date, label text)");
        cql.execute("CREATE TABLE ks.typed (id int PRIMARY KEY, tags list<text>, codes set<int>, counts map<text, int>,"
                + " period frozen<period>, addr inet, big bigint, amount decimal, day date, at time,"
                + " scores map<text, double>)");
        for (int id = 1; id <= 2; id++) {
            cql.execute("INSERT INTO ks.typed (id, tags, codes, counts, period, addr, big, amount, day, at, scores)"
                    + " VALUES (" + id + ", ['a', 'b'], {3, 1}, {'k': 1}, {starts: '2026-09-01', label: 'q3'},"
                    + " '10.0.0.1', " + BIG + ", 12.50, '2026-09-01', '08:30:00.123456789', {'s': 1.5})");
        }

        pg("CREATE TABLE readings (device TEXT, ts TIMESTAMPTZ, value INTEGER, PRIMARY KEY (device, ts))");
        // Mixed-case name and a PostgreSQL-only column with a default
        pg("CREATE TABLE \"Typed\" (id INTEGER PRIMARY KEY, tags TEXT[], codes INTEGER[], counts JSONB, period JSONB,"
                + " addr INET, big BIGINT, amount NUMERIC, day DATE, at TIME, scores JSONB,"
                + " ingested_at TIMESTAMPTZ NOT NULL DEFAULT now())");

        connectionFactory = new PostgresConnectionFactory(readings());
    }

    @AfterAll
    static void teardown() {
        if (connectionFactory != null)
            connectionFactory.close();
        if (cql != null)
            cql.close();
        PropertyHelper.destroyInstance();
    }

    @BeforeEach
    void truncateTargets() throws SQLException {
        pg("TRUNCATE readings, \"Typed\"");
    }

    @Test
    void migrate_copiesFilteredRowsWithExactTimestamps() throws SQLException {
        JobCounter counter = migrate(readings()).getJobCounter();

        assertEquals(expectedReadings(), targetReadings());
        assertEquals(expectedReadings().size(), counter.getCount(CounterType.WRITE));
        assertEquals(1, counter.getCount(CounterType.PARTITIONS_PASSED));
    }

    // Each device's rows arrive in ts order with values 0, 1, 2, so value 2 is always the third row and lands in the
    // second batch of 2
    @Test
    void migrate_failingBatch_failsPartitionAndCountsOnlyCommittedRows() throws SQLException {
        pg("ALTER TABLE readings ADD CONSTRAINT no_two CHECK (value <> 2)");
        try {
            JobCounter counter = migrate(readings()).getJobCounter();

            assertEquals(1, counter.getCount(CounterType.PARTITIONS_FAILED));
            assertEquals(0, counter.getCount(CounterType.PARTITIONS_PASSED));
            // transactionSize 2: the first batch was committed before the second failed
            assertEquals(2, counter.getCount(CounterType.WRITE));
            assertEquals(2, targetReadings().size());
            assertEquals(counter.getCount(CounterType.READ) - counter.getCount(CounterType.WRITE),
                    counter.getCount(CounterType.ERROR));
        } finally {
            pg("ALTER TABLE readings DROP CONSTRAINT no_two");
        }
    }

    @Test
    void migrate_failingBatch_rollsBackBatchesExecutedButNotCommitted() throws SQLException {
        pg("ALTER TABLE readings ADD CONSTRAINT no_two CHECK (value <> 2)");
        try {
            // transactionSize 4: the first batch has run but is not committed when the second fails
            JobCounter counter = migrate(readings("spark.cdm.connect.target.postgres.transactionSize", "4"))
                    .getJobCounter();

            assertEquals(1, counter.getCount(CounterType.PARTITIONS_FAILED));
            assertEquals(0, counter.getCount(CounterType.WRITE));
            assertEquals(0, targetReadings().size());
        } finally {
            pg("ALTER TABLE readings DROP CONSTRAINT no_two");
        }
    }

    @Test
    void validate_reportsMissingRowsBeforeMigrationAndValidRowsAfter() throws SQLException {
        JobCounter before = validate(readings()).getJobCounter();
        assertEquals(expectedReadings().size(), before.getCount(CounterType.MISSING));
        assertEquals(0, before.getCount(CounterType.VALID));

        migrate(readings());

        JobCounter after = validate(readings()).getJobCounter();
        assertEquals(expectedReadings().size(), after.getCount(CounterType.VALID));
        assertEquals(0, after.getCount(CounterType.MISSING));
        assertEquals(0, after.getCount(CounterType.MISMATCH));
    }

    @Test
    void validate_withAutocorrect_restoresMismatchedAndMissingRows() throws SQLException {
        migrate(readings());
        pg("UPDATE readings SET value = 42 WHERE device = 'dev-1' AND value = 0");
        pg("DELETE FROM readings WHERE device = 'dev-2' AND value = 0");

        JobCounter counter = validate(
                readings("spark.cdm.autocorrect.missing", "true", "spark.cdm.autocorrect.mismatch", "true"))
                        .getJobCounter();

        assertEquals(1, counter.getCount(CounterType.MISMATCH));
        assertEquals(1, counter.getCount(CounterType.MISSING));
        assertEquals(1, counter.getCount(CounterType.CORRECTED_MISMATCH));
        assertEquals(1, counter.getCount(CounterType.CORRECTED_MISSING));
        assertEquals(expectedReadings(), targetReadings());
    }

    @Test
    void typedColumns_migrate_writesEveryColumnAndKeepsPostgresDefaults() throws SQLException {
        JobCounter migrated = migrate(typed()).getJobCounter();

        assertEquals(1, migrated.getCount(CounterType.PARTITIONS_PASSED));
        assertEquals(2, migrated.getCount(CounterType.WRITE));
        assertEquals(2, count("SELECT count(*) FROM \"Typed\" WHERE ingested_at IS NOT NULL"
                + " AND period = '{\"starts\": \"2026-09-01\", \"label\": \"q3\"}'"));
    }

    @Test
    void typedColumns_validate_comparesEveryColumnExactly() throws SQLException {
        migrate(typed());

        JobCounter equal = validate(typed()).getJobCounter();
        assertEquals(2, equal.getCount(CounterType.VALID));
        assertEquals(0, equal.getCount(CounterType.MISMATCH));

        // 2^53 + 1 and 2^53 are the same double, so only an exact compare sees this change
        pg("UPDATE \"Typed\" SET big = big - 1 WHERE id = 1");
        JobCounter changed = validate(typed()).getJobCounter();
        assertEquals(1, changed.getCount(CounterType.MISMATCH));
        assertEquals(1, changed.getCount(CounterType.VALID));
    }

    @Test
    void migrate_valueThatCannotBeConverted_failsPartitionInsteadOfWritingNull() throws SQLException {
        // jsonb cannot store NaN. The cast picks the positional overload over the named-values Map one
        cql.execute(SimpleStatement.newInstance("INSERT INTO ks.typed (id, scores) VALUES (3, ?)",
                (Object) Collections.singletonMap("s", Double.NaN)));
        try {
            JobCounter counter = migrate(typed()).getJobCounter();

            assertEquals(1, counter.getCount(CounterType.PARTITIONS_FAILED));
            assertEquals(0, count("SELECT count(*) FROM \"Typed\" WHERE id = 3"));
            assertEquals(count("SELECT count(*) FROM \"Typed\""), counter.getCount(CounterType.WRITE));
        } finally {
            cql.execute("DELETE FROM ks.typed WHERE id = 3");
        }
    }

    private static PropertyHelper readings(String... extra) {
        String[] filtered = new String[extra.length + 2];
        filtered[0] = "spark.cdm.filter.cassandra.whereCondition";
        filtered[1] = "AND ts < '2026-09-02 00:00:00+0000'";
        System.arraycopy(extra, 0, filtered, 2, extra.length);
        return properties("ks.readings", "readings", filtered);
    }

    private static PropertyHelper typed() {
        return properties("ks.typed", "Typed");
    }

    // PropertyHelper loads its SparkConf only once, so each configuration needs a fresh instance
    private static PropertyHelper properties(String keyspaceTable, String pgTable, String... extra) {
        SparkConf conf = new SparkConf().set("spark.cdm.schema.origin.keyspaceTable", keyspaceTable)
                .set("spark.cdm.connect.target.type", "postgres")
                .set("spark.cdm.connect.target.postgres.url", postgres.getJdbcUrl())
                .set("spark.cdm.connect.target.postgres.username", postgres.getUsername())
                .set("spark.cdm.connect.target.postgres.password", postgres.getPassword())
                .set("spark.cdm.connect.target.postgres.table", pgTable)
                .set("spark.cdm.connect.target.postgres.pool.size", "2")
                .set("spark.cdm.connect.target.postgres.batchSize", "2")
                .set("spark.cdm.connect.target.postgres.transactionSize", "2");
        for (int i = 0; i < extra.length; i += 2) {
            conf.set(extra[i], extra[i + 1]);
        }
        PropertyHelper.destroyInstance();
        return PropertyHelper.getInstance(conf);
    }

    private static PartitionRange migrate(PropertyHelper propertyHelper) {
        PartitionRange range = fullRange(JobType.MIGRATE);
        new PostgresCopyJobSession(cql, connectionFactory, propertyHelper).processPartitionRange(range);
        return range;
    }

    private static PartitionRange validate(PropertyHelper propertyHelper) {
        PartitionRange range = fullRange(JobType.VALIDATE);
        new PostgresDiffJobSession(cql, connectionFactory, propertyHelper).processPartitionRange(range);
        return range;
    }

    private static PartitionRange fullRange(JobType jobType) {
        return new PartitionRange(BigInteger.valueOf(Long.MIN_VALUE), BigInteger.valueOf(Long.MAX_VALUE), jobType);
    }

    private static void pg(String sql) throws SQLException {
        try (Connection connection = postgres.createConnection(""); Statement stmt = connection.createStatement()) {
            stmt.execute(sql);
        }
    }

    private static long count(String sql) throws SQLException {
        try (Connection connection = postgres.createConnection(""); Statement stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {
            rs.next();
            return rs.getLong(1);
        }
    }

    private static List<String> expectedReadings() {
        List<String> rows = new ArrayList<>();
        for (String device : DEVICES) {
            for (int i = 0; i < ROWS_PER_DEVICE_IN_FILTER; i++) {
                rows.add(device + "|" + DAY_START.plus(Duration.ofHours(i)) + "|" + i);
            }
        }
        return rows;
    }

    private static List<String> targetReadings() throws SQLException {
        List<String> rows = new ArrayList<>();
        try (Connection connection = postgres.createConnection(""); Statement stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT device, ts, value FROM readings ORDER BY device, ts")) {
            while (rs.next()) {
                rows.add(rs.getString("device") + "|" + rs.getObject("ts", OffsetDateTime.class).toInstant() + "|"
                        + rs.getInt("value"));
            }
        }
        return rows;
    }
}
