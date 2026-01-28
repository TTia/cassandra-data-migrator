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

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.SQLException;

import org.apache.logging.log4j.ThreadContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.cdm.connect.PostgresConnectionFactory;
import com.datastax.cdm.cql.statement.OriginSelectByPartitionRangeStatement;
import com.datastax.cdm.cql.statement.PostgresUpsertStatement;
import com.datastax.cdm.data.PKFactory;
import com.datastax.cdm.data.Record;
import com.datastax.cdm.feature.TrackRun;
import com.datastax.cdm.properties.PropertyHelper;
import com.datastax.cdm.schema.CqlTable;
import com.datastax.cdm.schema.PostgresTable;
import com.datastax.cdm.schema.PostgresTypeMapper;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;

/**
 * Job session for copying data from Cassandra to PostgreSQL. Reads from Cassandra origin and writes to PostgreSQL
 * target.
 */
public class PostgresCopyJobSession extends AbstractJobSession<PartitionRange> {

    public Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    protected final PostgresConnectionFactory postgresConnectionFactory;
    protected final PostgresTable postgresTable;
    protected final PostgresTypeMapper typeMapper;
    protected final PKFactory pkFactory;
    protected final Integer fetchSize;

    /**
     * Creates a new PostgresCopyJobSession.
     *
     * @param originSession
     *            the Cassandra origin session
     * @param postgresConnectionFactory
     *            the PostgreSQL connection factory
     * @param propHelper
     *            the property helper
     */
    protected PostgresCopyJobSession(CqlSession originSession, PostgresConnectionFactory postgresConnectionFactory,
            PropertyHelper propHelper) {
        // Call parent with null targetSession since we use PostgreSQL via JDBC
        super(originSession, null, propHelper);

        this.postgresConnectionFactory = postgresConnectionFactory;
        this.typeMapper = new PostgresTypeMapper();

        // Initialize PostgreSQL target table
        this.postgresTable = new PostgresTable(propertyHelper);

        // Load PostgreSQL table metadata
        try (Connection conn = postgresConnectionFactory.getConnection()) {
            postgresTable.loadMetadata(conn);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to load PostgreSQL table metadata", e);
        }

        // Initialize PKFactory (for origin side processing)
        // Target side uses PostgreSQL, so we only need origin PKFactory
        CqlTable cqlTableOrigin = this.originSession.getCqlTable();
        this.pkFactory = new PKFactory(propertyHelper, cqlTableOrigin, null);
        this.originSession.setPKFactory(pkFactory);

        this.fetchSize = cqlTableOrigin.getFetchSizeInRows();

        logger.info("PostgresCopyJobSession initialized:");
        logger.info("  Origin: {}", cqlTableOrigin.getKeyspaceTable());
        logger.info("  Target: {}", postgresTable.getQualifiedTableName());
        logger.info("  CQL -- origin select: {}",
                this.originSession.getOriginSelectByPartitionRangeStatement().getCQL());
    }

    @Override
    public void processPartitionRange(PartitionRange range, TrackRun trackRunFeature, long runId) {
        this.trackRunFeature = trackRunFeature;
        this.runId = runId;
        this.processPartitionRange(range);
    }

    /**
     * Process a partition range by reading from Cassandra and writing to PostgreSQL.
     *
     * @param range
     *            the partition range to process
     */
    protected void processPartitionRange(PartitionRange range) {
        BigInteger min = range.getMin(), max = range.getMax();
        ThreadContext.put(THREAD_CONTEXT_LABEL, getThreadLabel(min, max));
        logger.info("ThreadID: {} Processing min: {} max: {}", Thread.currentThread().getId(), min, max);
        if (null != trackRunFeature) {
            trackRunFeature.updateCdmRun(runId, min, TrackRun.RUN_STATUS.STARTED, "");
        }

        JobCounter jobCounter = range.getJobCounter();

        try (Connection connection = postgresConnectionFactory.getConnection()) {
            // Create PostgreSQL upsert statement
            PostgresUpsertStatement upsertStatement = new PostgresUpsertStatement(postgresTable,
                    originSession.getCqlTable(), typeMapper, propertyHelper);
            upsertStatement.initialize(connection);

            try {
                OriginSelectByPartitionRangeStatement originSelectStatement = this.originSession
                        .getOriginSelectByPartitionRangeStatement();
                ResultSet resultSet = originSelectStatement.execute(originSelectStatement.bind(min, max));

                for (Row originRow : resultSet) {
                    rateLimiterOrigin.acquire(1);
                    jobCounter.increment(JobCounter.CounterType.READ);

                    Record record = new Record(pkFactory.getTargetPK(originRow), originRow, null);
                    if (originSelectStatement.shouldFilterRecord(record)) {
                        jobCounter.increment(JobCounter.CounterType.SKIPPED);
                        continue;
                    }

                    for (Record r : pkFactory.toValidRecordList(record)) {
                        try {
                            rateLimiterTarget.acquire(1);
                            upsertStatement.addToBatch(r);
                            jobCounter.increment(JobCounter.CounterType.UNFLUSHED);

                            // Execute batch if size threshold reached
                            if (upsertStatement.shouldExecuteBatch()) {
                                upsertStatement.executeBatch();
                                jobCounter.increment(JobCounter.CounterType.WRITE,
                                        jobCounter.getCount(JobCounter.CounterType.UNFLUSHED, true));
                                jobCounter.reset(JobCounter.CounterType.UNFLUSHED);

                                // Commit if transaction size threshold reached
                                if (upsertStatement.shouldCommit()) {
                                    upsertStatement.commit();
                                }
                            }
                        } catch (SQLException e) {
                            logger.error("Error writing record to PostgreSQL: {}", e.getMessage());
                            jobCounter.increment(JobCounter.CounterType.ERROR);
                        }
                    }
                }

                // Flush remaining batch
                if (upsertStatement.getCurrentBatchCount() > 0) {
                    upsertStatement.executeBatch();
                    jobCounter.increment(JobCounter.CounterType.WRITE,
                            jobCounter.getCount(JobCounter.CounterType.UNFLUSHED, true));
                    jobCounter.reset(JobCounter.CounterType.UNFLUSHED);
                }

                // Final commit
                upsertStatement.commit();

                jobCounter.increment(JobCounter.CounterType.PARTITIONS_PASSED);
                jobCounter.flush();

                if (null != trackRunFeature) {
                    trackRunFeature.updateCdmRun(runId, min, TrackRun.RUN_STATUS.PASS, jobCounter.getMetrics());
                }
            } catch (SQLException e) {
                // Rollback on error
                try {
                    upsertStatement.rollback();
                } catch (SQLException rollbackEx) {
                    logger.error("Error rolling back transaction", rollbackEx);
                }
                throw e;
            } finally {
                try {
                    upsertStatement.close();
                } catch (SQLException e) {
                    logger.error("Error closing upsert statement", e);
                }
            }
        } catch (Exception e) {
            jobCounter.increment(JobCounter.CounterType.ERROR,
                    jobCounter.getCount(JobCounter.CounterType.READ, true)
                            - jobCounter.getCount(JobCounter.CounterType.WRITE, true)
                            - jobCounter.getCount(JobCounter.CounterType.SKIPPED, true));
            jobCounter.increment(JobCounter.CounterType.PARTITIONS_FAILED);
            logger.error("Error with PartitionRange -- ThreadID: {} Processing min: {} max: {}",
                    Thread.currentThread().getId(), min, max, e);
            logger.error("Error stats " + jobCounter.getMetrics(true));
            jobCounter.flush();

            if (null != trackRunFeature) {
                trackRunFeature.updateCdmRun(runId, min, TrackRun.RUN_STATUS.FAIL, jobCounter.getMetrics());
            }
        }
    }

    /**
     * Gets the PostgreSQL connection factory.
     *
     * @return the connection factory
     */
    public PostgresConnectionFactory getPostgresConnectionFactory() {
        return postgresConnectionFactory;
    }

    /**
     * Gets the PostgreSQL table.
     *
     * @return the PostgreSQL table
     */
    public PostgresTable getPostgresTable() {
        return postgresTable;
    }
}
