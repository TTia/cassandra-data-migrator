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

import java.sql.Connection;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.cdm.properties.IPropertyHelper;
import com.datastax.cdm.properties.KnownProperties;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

/**
 * Manages JDBC connection pooling for PostgreSQL target database. Uses HikariCP for high-performance connection
 * pooling.
 */
public class PostgresConnectionFactory implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(PostgresConnectionFactory.class);

    private final HikariDataSource dataSource;
    private final String jdbcUrl;
    private final String schema;
    private final String table;

    /**
     * Creates a new PostgresConnectionFactory with connection pooling.
     *
     * @param propertyHelper
     *            the property helper to read configuration from
     *
     * @throws IllegalArgumentException
     *             if required properties are missing
     */
    public PostgresConnectionFactory(IPropertyHelper propertyHelper) {
        this.jdbcUrl = propertyHelper.getString(KnownProperties.PG_JDBC_URL);
        if (jdbcUrl == null || jdbcUrl.isEmpty()) {
            throw new IllegalArgumentException("PostgreSQL JDBC URL is required: " + KnownProperties.PG_JDBC_URL);
        }

        String username = propertyHelper.getString(KnownProperties.PG_USERNAME);
        String password = propertyHelper.getString(KnownProperties.PG_PASSWORD);
        this.schema = propertyHelper.getString(KnownProperties.PG_SCHEMA);
        this.table = propertyHelper.getString(KnownProperties.PG_TABLE);

        Integer poolSize = propertyHelper.getInteger(KnownProperties.PG_POOL_SIZE);
        Integer poolTimeout = propertyHelper.getInteger(KnownProperties.PG_POOL_TIMEOUT);

        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(jdbcUrl);
        config.setDriverClassName("org.postgresql.Driver");

        if (username != null && !username.isEmpty()) {
            config.setUsername(username);
        }
        if (password != null && !password.isEmpty()) {
            config.setPassword(password);
        }

        // Pool configuration
        config.setMaximumPoolSize(poolSize != null ? poolSize : 10);
        config.setConnectionTimeout(poolTimeout != null ? poolTimeout : 30000);
        config.setIdleTimeout(600000); // 10 minutes
        config.setMaxLifetime(1800000); // 30 minutes
        config.setMinimumIdle(2);

        // PostgreSQL-specific optimizations
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "250");
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
        config.addDataSourceProperty("reWriteBatchedInserts", "true");

        // Pool name for monitoring
        config.setPoolName("CDM-PostgreSQL-Pool");

        logger.info("Creating PostgreSQL connection pool: url={}, schema={}, table={}, poolSize={}", jdbcUrl, schema,
                table, config.getMaximumPoolSize());

        this.dataSource = new HikariDataSource(config);
    }

    /**
     * Gets a connection from the pool.
     *
     * @return a database connection
     *
     * @throws SQLException
     *             if unable to get a connection
     */
    public Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

    /**
     * Returns the JDBC URL being used.
     *
     * @return the JDBC URL
     */
    public String getJdbcUrl() {
        return jdbcUrl;
    }

    /**
     * Returns the PostgreSQL schema name.
     *
     * @return the schema name
     */
    public String getSchema() {
        return schema;
    }

    /**
     * Returns the PostgreSQL table name.
     *
     * @return the table name
     */
    public String getTable() {
        return table;
    }

    /**
     * Returns statistics about the connection pool.
     *
     * @return pool statistics
     */
    public PoolStats getPoolStats() {
        return new PoolStats(dataSource.getHikariPoolMXBean().getActiveConnections(),
                dataSource.getHikariPoolMXBean().getIdleConnections(),
                dataSource.getHikariPoolMXBean().getTotalConnections(),
                dataSource.getHikariPoolMXBean().getThreadsAwaitingConnection());
    }

    /**
     * Tests if the connection pool can establish a connection.
     *
     * @return true if a connection can be obtained
     */
    public boolean testConnection() {
        try (Connection conn = getConnection()) {
            return conn.isValid(5);
        } catch (SQLException e) {
            logger.error("Failed to test PostgreSQL connection", e);
            return false;
        }
    }

    /**
     * Checks if the factory is closed.
     *
     * @return true if closed
     */
    public boolean isClosed() {
        return dataSource.isClosed();
    }

    @Override
    public void close() {
        if (dataSource != null && !dataSource.isClosed()) {
            logger.info("Closing PostgreSQL connection pool");
            dataSource.close();
        }
    }

    /**
     * Statistics about the connection pool.
     */
    public static class PoolStats {
        private final int activeConnections;
        private final int idleConnections;
        private final int totalConnections;
        private final int threadsAwaitingConnection;

        public PoolStats(int activeConnections, int idleConnections, int totalConnections,
                int threadsAwaitingConnection) {
            this.activeConnections = activeConnections;
            this.idleConnections = idleConnections;
            this.totalConnections = totalConnections;
            this.threadsAwaitingConnection = threadsAwaitingConnection;
        }

        public int getActiveConnections() {
            return activeConnections;
        }

        public int getIdleConnections() {
            return idleConnections;
        }

        public int getTotalConnections() {
            return totalConnections;
        }

        public int getThreadsAwaitingConnection() {
            return threadsAwaitingConnection;
        }

        @Override
        public String toString() {
            return String.format("PoolStats{active=%d, idle=%d, total=%d, waiting=%d}", activeConnections,
                    idleConnections, totalConnections, threadsAwaitingConnection);
        }
    }
}
