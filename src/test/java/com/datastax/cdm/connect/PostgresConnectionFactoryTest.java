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
package com.datastax.cdm.connect;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.SQLException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import com.datastax.cdm.properties.IPropertyHelper;
import com.datastax.cdm.properties.KnownProperties;

@Testcontainers
@EnabledIfEnvironmentVariable(named = "RUN_INTEGRATION_TESTS", matches = "true")
public class PostgresConnectionFactoryTest {

    private static final String TIMESCALE_IMAGE = "timescale/timescaledb:latest-pg16";

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(
            DockerImageName.parse(TIMESCALE_IMAGE).asCompatibleSubstituteFor("postgres")).withDatabaseName("testdb")
                    .withUsername("testuser").withPassword("testpass");

    @Mock
    private IPropertyHelper propertyHelper;

    @BeforeEach
    public void setup() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void constructor_missingJdbcUrl_throwsException() {
        when(propertyHelper.getString(KnownProperties.PG_JDBC_URL)).thenReturn(null);

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> new PostgresConnectionFactory(propertyHelper));

        assertTrue(exception.getMessage().contains("PostgreSQL JDBC URL is required"));
    }

    @Test
    public void constructor_emptyJdbcUrl_throwsException() {
        when(propertyHelper.getString(KnownProperties.PG_JDBC_URL)).thenReturn("");

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> new PostgresConnectionFactory(propertyHelper));

        assertTrue(exception.getMessage().contains("PostgreSQL JDBC URL is required"));
    }

    @Test
    public void constructor_validConfiguration_createsFactory() {
        when(propertyHelper.getString(KnownProperties.PG_JDBC_URL)).thenReturn(postgres.getJdbcUrl());
        when(propertyHelper.getString(KnownProperties.PG_USERNAME)).thenReturn(postgres.getUsername());
        when(propertyHelper.getString(KnownProperties.PG_PASSWORD)).thenReturn(postgres.getPassword());
        when(propertyHelper.getString(KnownProperties.PG_SCHEMA)).thenReturn("public");
        when(propertyHelper.getString(KnownProperties.PG_TABLE)).thenReturn("test_table");
        when(propertyHelper.getInteger(KnownProperties.PG_POOL_SIZE)).thenReturn(5);
        when(propertyHelper.getInteger(KnownProperties.PG_POOL_TIMEOUT)).thenReturn(10000);

        PostgresConnectionFactory factory = new PostgresConnectionFactory(propertyHelper);

        assertNotNull(factory);
        assertEquals(postgres.getJdbcUrl(), factory.getJdbcUrl());
        assertEquals("public", factory.getSchema());
        assertEquals("test_table", factory.getTable());
        assertFalse(factory.isClosed());

        factory.close();
        assertTrue(factory.isClosed());
    }

    @Test
    public void constructor_nullPoolSettings_usesDefaults() {
        when(propertyHelper.getString(KnownProperties.PG_JDBC_URL)).thenReturn(postgres.getJdbcUrl());
        when(propertyHelper.getString(KnownProperties.PG_USERNAME)).thenReturn(postgres.getUsername());
        when(propertyHelper.getString(KnownProperties.PG_PASSWORD)).thenReturn(postgres.getPassword());
        when(propertyHelper.getString(KnownProperties.PG_SCHEMA)).thenReturn(null);
        when(propertyHelper.getString(KnownProperties.PG_TABLE)).thenReturn(null);
        when(propertyHelper.getInteger(KnownProperties.PG_POOL_SIZE)).thenReturn(null);
        when(propertyHelper.getInteger(KnownProperties.PG_POOL_TIMEOUT)).thenReturn(null);

        PostgresConnectionFactory factory = new PostgresConnectionFactory(propertyHelper);

        assertNotNull(factory);
        assertFalse(factory.isClosed());

        factory.close();
    }

    @Test
    public void poolStats_toString_formatsCorrectly() {
        PostgresConnectionFactory.PoolStats stats = new PostgresConnectionFactory.PoolStats(5, 3, 8, 2);

        assertEquals(5, stats.getActiveConnections());
        assertEquals(3, stats.getIdleConnections());
        assertEquals(8, stats.getTotalConnections());
        assertEquals(2, stats.getThreadsAwaitingConnection());

        String str = stats.toString();
        assertTrue(str.contains("active=5"));
        assertTrue(str.contains("idle=3"));
        assertTrue(str.contains("total=8"));
        assertTrue(str.contains("waiting=2"));
    }

    @Test
    public void close_calledMultipleTimes_noError() {
        when(propertyHelper.getString(KnownProperties.PG_JDBC_URL)).thenReturn(postgres.getJdbcUrl());
        when(propertyHelper.getString(KnownProperties.PG_USERNAME)).thenReturn(postgres.getUsername());
        when(propertyHelper.getString(KnownProperties.PG_PASSWORD)).thenReturn(postgres.getPassword());
        when(propertyHelper.getString(KnownProperties.PG_SCHEMA)).thenReturn("public");
        when(propertyHelper.getString(KnownProperties.PG_TABLE)).thenReturn("test");
        when(propertyHelper.getInteger(KnownProperties.PG_POOL_SIZE)).thenReturn(2);
        when(propertyHelper.getInteger(KnownProperties.PG_POOL_TIMEOUT)).thenReturn(5000);

        PostgresConnectionFactory factory = new PostgresConnectionFactory(propertyHelper);

        factory.close();
        factory.close(); // Should not throw

        assertTrue(factory.isClosed());
    }

    @Test
    public void getConnection_returnsValidConnection() throws SQLException {
        when(propertyHelper.getString(KnownProperties.PG_JDBC_URL)).thenReturn(postgres.getJdbcUrl());
        when(propertyHelper.getString(KnownProperties.PG_USERNAME)).thenReturn(postgres.getUsername());
        when(propertyHelper.getString(KnownProperties.PG_PASSWORD)).thenReturn(postgres.getPassword());
        when(propertyHelper.getString(KnownProperties.PG_SCHEMA)).thenReturn("public");
        when(propertyHelper.getString(KnownProperties.PG_TABLE)).thenReturn("test");
        when(propertyHelper.getInteger(KnownProperties.PG_POOL_SIZE)).thenReturn(5);
        when(propertyHelper.getInteger(KnownProperties.PG_POOL_TIMEOUT)).thenReturn(30000);

        PostgresConnectionFactory factory = new PostgresConnectionFactory(propertyHelper);

        try (Connection conn = factory.getConnection()) {
            assertNotNull(conn);
            assertFalse(conn.isClosed());
        }

        factory.close();
    }

    @Test
    public void getPoolStats_returnsStats() {
        when(propertyHelper.getString(KnownProperties.PG_JDBC_URL)).thenReturn(postgres.getJdbcUrl());
        when(propertyHelper.getString(KnownProperties.PG_USERNAME)).thenReturn(postgres.getUsername());
        when(propertyHelper.getString(KnownProperties.PG_PASSWORD)).thenReturn(postgres.getPassword());
        when(propertyHelper.getString(KnownProperties.PG_SCHEMA)).thenReturn("public");
        when(propertyHelper.getString(KnownProperties.PG_TABLE)).thenReturn("test");
        when(propertyHelper.getInteger(KnownProperties.PG_POOL_SIZE)).thenReturn(5);
        when(propertyHelper.getInteger(KnownProperties.PG_POOL_TIMEOUT)).thenReturn(30000);

        PostgresConnectionFactory factory = new PostgresConnectionFactory(propertyHelper);

        PostgresConnectionFactory.PoolStats stats = factory.getPoolStats();
        assertNotNull(stats);
        assertTrue(stats.getTotalConnections() >= 0);

        factory.close();
    }

    @Test
    public void testConnection_validConnection_returnsTrue() {
        when(propertyHelper.getString(KnownProperties.PG_JDBC_URL)).thenReturn(postgres.getJdbcUrl());
        when(propertyHelper.getString(KnownProperties.PG_USERNAME)).thenReturn(postgres.getUsername());
        when(propertyHelper.getString(KnownProperties.PG_PASSWORD)).thenReturn(postgres.getPassword());
        when(propertyHelper.getString(KnownProperties.PG_SCHEMA)).thenReturn("public");
        when(propertyHelper.getString(KnownProperties.PG_TABLE)).thenReturn("test");
        when(propertyHelper.getInteger(KnownProperties.PG_POOL_SIZE)).thenReturn(5);
        when(propertyHelper.getInteger(KnownProperties.PG_POOL_TIMEOUT)).thenReturn(30000);

        PostgresConnectionFactory factory = new PostgresConnectionFactory(propertyHelper);

        assertTrue(factory.testConnection());

        factory.close();
    }
}
