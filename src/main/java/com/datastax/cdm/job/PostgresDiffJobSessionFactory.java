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

import java.io.Serializable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.cdm.connect.PostgresConnectionFactory;
import com.datastax.cdm.properties.KnownProperties;
import com.datastax.cdm.properties.PropertyHelper;
import com.datastax.oss.driver.api.core.CqlSession;

/**
 * Factory for creating PostgresDiffJobSession instances. Used when the target database type is PostgreSQL for
 * validation/diff jobs.
 */
public class PostgresDiffJobSessionFactory implements IJobSessionFactory<PartitionRange>, Serializable {

    private static final long serialVersionUID = 1L;
    private static final Logger logger = LoggerFactory.getLogger(PostgresDiffJobSessionFactory.class);

    private static PostgresDiffJobSession jobSession = null;
    private static PostgresConnectionFactory connectionFactory = null;

    /**
     * Gets a singleton instance of PostgresDiffJobSession. Implements IJobSessionFactory interface. The targetSession
     * parameter is ignored since PostgreSQL connections are managed via JDBC, not CqlSession.
     *
     * @param originSession
     *            the Cassandra origin session
     * @param targetSession
     *            ignored for PostgreSQL targets (uses JDBC instead)
     * @param propHelper
     *            the property helper
     *
     * @return the job session instance
     */
    @Override
    public AbstractJobSession<PartitionRange> getInstance(CqlSession originSession, CqlSession targetSession,
            PropertyHelper propHelper) {
        if (jobSession == null) {
            synchronized (PostgresDiffJobSession.class) {
                if (jobSession == null) {
                    logger.info("Creating PostgresDiffJobSession");
                    connectionFactory = new PostgresConnectionFactory(propHelper);
                    jobSession = new PostgresDiffJobSession(originSession, connectionFactory, propHelper);
                }
            }
        }
        return jobSession;
    }

    /**
     * Gets the PostgreSQL connection factory.
     *
     * @return the connection factory, or null if not initialized
     */
    public PostgresConnectionFactory getConnectionFactory() {
        return connectionFactory;
    }

    /**
     * Checks if the target type is PostgreSQL based on configuration.
     *
     * @param propHelper
     *            the property helper
     *
     * @return true if target type is PostgreSQL
     */
    public static boolean isPostgresTarget(PropertyHelper propHelper) {
        String targetType = propHelper.getString(KnownProperties.TARGET_TYPE);
        return "postgres".equalsIgnoreCase(targetType) || "postgresql".equalsIgnoreCase(targetType);
    }

    /**
     * Shuts down the factory and releases resources.
     */
    public static void shutdown() {
        if (connectionFactory != null) {
            connectionFactory.close();
            connectionFactory = null;
        }
        jobSession = null;
    }
}
