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
 * Runs the PostgreSQL copy and diff sessions end to end: a real Cassandra origin, a real PostgreSQL target.
 */
@Testcontainers(disabledWithoutDocker = true)
public class PostgresJobSessionTest {

    private static final String TIMESCALE_IMAGE = "timescale/timescaledb:latest-pg16";
    private static final Instant DAY_START = Instant.parse("2026-09-01T00:00:00.123Z");
    private static final Instant OUT_OF_FILTER = Instant.parse("2026-09-10T00:00:00Z");
    private static final int ROWS_PER_DEVICE_IN_FILTER = 3;
    private static final String[] DEVICES = { "dev-1", "dev-2" };

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
    private static PropertyHelper propertyHelper;
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
                insert(device, DAY_START.plus(Duration.ofHours(i)), i);
            }
            insert(device, OUT_OF_FILTER, 99);
        }

        try (Connection connection = postgres.createConnection(""); Statement stmt = connection.createStatement()) {
            stmt.execute(
                    "CREATE TABLE readings (device TEXT, ts TIMESTAMPTZ, value INTEGER, PRIMARY KEY (device, ts))");
        }

        PropertyHelper.destroyInstance();
        SparkConf conf = new SparkConf().set("spark.cdm.schema.origin.keyspaceTable", "ks.readings")
                .set("spark.cdm.filter.cassandra.whereCondition", "AND ts < '2026-09-02 00:00:00+0000'")
                .set("spark.cdm.connect.target.type", "postgres")
                .set("spark.cdm.connect.target.postgres.url", postgres.getJdbcUrl())
                .set("spark.cdm.connect.target.postgres.username", postgres.getUsername())
                .set("spark.cdm.connect.target.postgres.password", postgres.getPassword())
                .set("spark.cdm.connect.target.postgres.table", "readings")
                .set("spark.cdm.connect.target.postgres.pool.size", "2");
        propertyHelper = PropertyHelper.getInstance(conf);
        connectionFactory = new PostgresConnectionFactory(propertyHelper);
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
    void truncateTarget() throws SQLException {
        try (Connection connection = postgres.createConnection(""); Statement stmt = connection.createStatement()) {
            stmt.execute("TRUNCATE readings");
        }
    }

    @Test
    void migrate_copiesFilteredRowsWithExactTimestamps() throws SQLException {
        PartitionRange range = migrate();

        assertEquals(expectedRows(), targetRows());
        assertEquals(expectedRows().size(), range.getJobCounter().getCount(CounterType.WRITE));
    }

    @Test
    void validate_reportsMissingRowsBeforeMigrationAndValidRowsAfter() {
        JobCounter before = validate().getJobCounter();
        assertEquals(expectedRows().size(), before.getCount(CounterType.MISSING));
        assertEquals(0, before.getCount(CounterType.VALID));

        migrate();

        JobCounter after = validate().getJobCounter();
        assertEquals(expectedRows().size(), after.getCount(CounterType.VALID));
        assertEquals(0, after.getCount(CounterType.MISSING));
        assertEquals(0, after.getCount(CounterType.MISMATCH));
    }

    private static PartitionRange migrate() {
        PartitionRange range = fullRange(JobType.MIGRATE);
        new PostgresCopyJobSession(cql, connectionFactory, propertyHelper).processPartitionRange(range);
        return range;
    }

    private static PartitionRange validate() {
        PartitionRange range = fullRange(JobType.VALIDATE);
        new PostgresDiffJobSession(cql, connectionFactory, propertyHelper).processPartitionRange(range);
        return range;
    }

    private static PartitionRange fullRange(JobType jobType) {
        return new PartitionRange(BigInteger.valueOf(Long.MIN_VALUE), BigInteger.valueOf(Long.MAX_VALUE), jobType);
    }

    private static void insert(String device, Instant ts, int value) {
        cql.execute(SimpleStatement.newInstance("INSERT INTO ks.readings (device, ts, value) VALUES (?, ?, ?)", device,
                ts, value));
    }

    private static List<String> expectedRows() {
        List<String> rows = new ArrayList<>();
        for (String device : DEVICES) {
            for (int i = 0; i < ROWS_PER_DEVICE_IN_FILTER; i++) {
                rows.add(device + "|" + DAY_START.plus(Duration.ofHours(i)) + "|" + i);
            }
        }
        return rows;
    }

    private static List<String> targetRows() throws SQLException {
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
