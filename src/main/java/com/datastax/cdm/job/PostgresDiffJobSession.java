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
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.ThreadContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.cdm.connect.PostgresConnectionFactory;
import com.datastax.cdm.cql.statement.OriginSelectByPartitionRangeStatement;
import com.datastax.cdm.cql.statement.PostgresSelectByPKStatement;
import com.datastax.cdm.cql.statement.PostgresUpsertStatement;
import com.datastax.cdm.data.PKFactory;
import com.datastax.cdm.data.Record;
import com.datastax.cdm.feature.TrackRun;
import com.datastax.cdm.properties.KnownProperties;
import com.datastax.cdm.properties.PropertyHelper;
import com.datastax.cdm.schema.CqlTable;
import com.datastax.cdm.schema.PostgresTable;
import com.datastax.cdm.schema.PostgresTypeMapper;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;

/**
 * Job session for validating data between Cassandra origin and PostgreSQL target. Compares records and optionally
 * auto-corrects missing or mismatched data.
 */
public class PostgresDiffJobSession extends AbstractJobSession<PartitionRange> {

    public Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    protected final PostgresConnectionFactory postgresConnectionFactory;
    protected final PostgresTable postgresTable;
    protected final PostgresTypeMapper typeMapper;
    protected final PKFactory pkFactory;
    protected final PostgresSelectByPKStatement selectByPKStatement;

    protected final Boolean autoCorrectMissing;
    protected final Boolean autoCorrectMismatch;

    /**
     * Creates a new PostgresDiffJobSession.
     *
     * @param originSession
     *            the Cassandra origin session
     * @param postgresConnectionFactory
     *            the PostgreSQL connection factory
     * @param propHelper
     *            the property helper
     */
    protected PostgresDiffJobSession(CqlSession originSession, PostgresConnectionFactory postgresConnectionFactory,
            PropertyHelper propHelper) {
        // Call parent with null targetSession since we use PostgreSQL via JDBC
        super(originSession, null, propHelper);

        this.postgresConnectionFactory = postgresConnectionFactory;
        this.typeMapper = new PostgresTypeMapper();

        // Load autocorrect settings
        this.autoCorrectMissing = propertyHelper.getBoolean(KnownProperties.AUTOCORRECT_MISSING);
        this.autoCorrectMismatch = propertyHelper.getBoolean(KnownProperties.AUTOCORRECT_MISMATCH);

        logger.info("PARAM -- Autocorrect Missing: {}", autoCorrectMissing);
        logger.info("PARAM -- Autocorrect Mismatch: {}", autoCorrectMismatch);

        // Initialize PostgreSQL target table
        this.postgresTable = new PostgresTable(propertyHelper);

        // Load PostgreSQL table metadata
        try (Connection conn = postgresConnectionFactory.getConnection()) {
            postgresTable.loadMetadata(conn);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to load PostgreSQL table metadata", e);
        }

        // PostgreSQL has no CqlTable, so the origin table stands in as the target: PK columns map one to one
        CqlTable cqlTableOrigin = this.originSession.getCqlTable();
        cqlTableOrigin.setOtherCqlTable(cqlTableOrigin);
        this.pkFactory = new PKFactory(propertyHelper, cqlTableOrigin, cqlTableOrigin);
        this.originSession.setPKFactory(pkFactory);
        this.selectByPKStatement = new PostgresSelectByPKStatement(postgresTable, cqlTableOrigin);

        logger.info("PostgresDiffJobSession initialized:");
        logger.info("  Origin: {}", cqlTableOrigin.getKeyspaceTable());
        logger.info("  Target: {}", postgresTable.getQualifiedTableName());
        logger.info("  CQL -- origin select: {}",
                this.originSession.getOriginSelectByPartitionRangeStatement().getCQL());
        logger.info("  SQL -- target select: {}", selectByPKStatement.getSelectStatement());
    }

    @Override
    public void processPartitionRange(PartitionRange range, TrackRun trackRunFeature, long runId) {
        this.trackRunFeature = trackRunFeature;
        this.runId = runId;
        this.processPartitionRange(range);
    }

    /**
     * Process a partition range by comparing Cassandra and PostgreSQL data.
     */
    protected void processPartitionRange(PartitionRange range) {
        BigInteger min = range.getMin(), max = range.getMax();
        ThreadContext.put(THREAD_CONTEXT_LABEL, getThreadLabel(min, max));
        logger.info("ThreadID: {} Processing min: {} max: {}", Thread.currentThread().getId(), min, max);
        if (null != trackRunFeature) {
            trackRunFeature.updateCdmRun(runId, min, TrackRun.RUN_STATUS.STARTED, "");
        }

        AtomicBoolean hasDiff = new AtomicBoolean(false);
        JobCounter jobCounter = range.getJobCounter();

        try (Connection connection = postgresConnectionFactory.getConnection()) {
            PostgresUpsertStatement upsertStatement = null;
            if (autoCorrectMissing || autoCorrectMismatch) {
                upsertStatement = new PostgresUpsertStatement(postgresTable, originSession.getCqlTable(), typeMapper,
                        propertyHelper);
                upsertStatement.initialize(connection);
            }

            try {
                OriginSelectByPartitionRangeStatement originSelectStatement = originSession
                        .getOriginSelectByPartitionRangeStatement();
                ResultSet resultSet = originSelectStatement.execute(originSelectStatement.bind(min, max));
                long correctedMissing = 0, correctedMismatch = 0;

                for (Row originRow : resultSet) {
                    rateLimiterOrigin.acquire(1);
                    jobCounter.increment(JobCounter.CounterType.READ);

                    Record record = new Record(pkFactory.getTargetPK(originRow), originRow, null);
                    if (originSelectStatement.shouldFilterRecord(record)) {
                        jobCounter.increment(JobCounter.CounterType.SKIPPED);
                        continue;
                    }

                    for (Record r : pkFactory.toValidRecordList(record)) {
                        rateLimiterTarget.acquire(1);

                        List<String> mismatches = selectByPKStatement.findMismatches(connection, r);
                        if (mismatches == null) {
                            jobCounter.increment(JobCounter.CounterType.MISSING);
                            logger.error("Missing target row found for key: {}", r.getPk());
                            hasDiff.set(true);
                            if (autoCorrectMissing) {
                                addCorrection(upsertStatement, r);
                                correctedMissing++;
                            }
                        } else if (!mismatches.isEmpty()) {
                            jobCounter.increment(JobCounter.CounterType.MISMATCH);
                            logger.error("Mismatch row found for key: {} Mismatch: {}", r.getPk(),
                                    String.join("; ", mismatches));
                            hasDiff.set(true);
                            if (autoCorrectMismatch) {
                                addCorrection(upsertStatement, r);
                                correctedMismatch++;
                            }
                        } else {
                            jobCounter.increment(JobCounter.CounterType.VALID);
                        }
                    }
                }

                // Always commit: batches executed mid-loop are not committed yet, and close() rolls them back
                if (upsertStatement != null) {
                    upsertStatement.executeBatch();
                    upsertStatement.commit();
                }
                // Counted only now, so a failed partition never reports a correction that was rolled back
                jobCounter.increment(JobCounter.CounterType.CORRECTED_MISSING, correctedMissing);
                jobCounter.increment(JobCounter.CounterType.CORRECTED_MISMATCH, correctedMismatch);

                jobCounter.increment(JobCounter.CounterType.PARTITIONS_PASSED);
                jobCounter.flush();

                if (hasDiff.get() && null != trackRunFeature) {
                    if (jobCounter.getCount(JobCounter.CounterType.MISSING) == jobCounter
                            .getCount(JobCounter.CounterType.CORRECTED_MISSING)
                            && jobCounter.getCount(JobCounter.CounterType.MISMATCH) == jobCounter
                                    .getCount(JobCounter.CounterType.CORRECTED_MISMATCH)) {
                        trackRunFeature.updateCdmRun(runId, min, TrackRun.RUN_STATUS.DIFF_CORRECTED,
                                jobCounter.getMetrics());
                    } else {
                        trackRunFeature.updateCdmRun(runId, min, TrackRun.RUN_STATUS.DIFF, jobCounter.getMetrics());
                    }
                } else if (null != trackRunFeature) {
                    trackRunFeature.updateCdmRun(runId, min, TrackRun.RUN_STATUS.PASS, jobCounter.getMetrics());
                }
            } finally {
                if (upsertStatement != null) {
                    try {
                        upsertStatement.close();
                    } catch (SQLException e) {
                        logger.error("Error closing upsert statement", e);
                    }
                }
            }
        } catch (Exception e) {
            jobCounter.increment(JobCounter.CounterType.ERROR,
                    jobCounter.getCount(JobCounter.CounterType.READ, true)
                            - jobCounter.getCount(JobCounter.CounterType.VALID, true)
                            - jobCounter.getCount(JobCounter.CounterType.MISSING, true)
                            - jobCounter.getCount(JobCounter.CounterType.MISMATCH, true)
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

    private static void addCorrection(PostgresUpsertStatement upsertStatement, Record record) throws SQLException {
        upsertStatement.addToBatch(record);
        if (upsertStatement.shouldExecuteBatch()) {
            upsertStatement.executeBatch();
        }
    }

    /**
     * Gets the PostgreSQL connection factory.
     */
    public PostgresConnectionFactory getPostgresConnectionFactory() {
        return postgresConnectionFactory;
    }
}
