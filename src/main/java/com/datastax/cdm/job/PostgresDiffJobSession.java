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
import java.util.Map;
import java.util.Objects;
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
import com.datastax.oss.driver.api.core.type.DataType;

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

    // Column metadata for comparison
    private final List<String> targetColumnNames;
    private final List<String> originColumnNames;
    private final List<DataType> originColumnTypes;

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

        // Initialize PKFactory and select statement
        CqlTable cqlTableOrigin = this.originSession.getCqlTable();
        this.pkFactory = new PKFactory(propertyHelper, cqlTableOrigin, null);
        this.originSession.setPKFactory(pkFactory);
        this.selectByPKStatement = new PostgresSelectByPKStatement(postgresTable, cqlTableOrigin);

        // Store column metadata for comparison
        this.targetColumnNames = postgresTable.getColumnNames();
        this.originColumnNames = cqlTableOrigin.getColumnNames(false);
        this.originColumnTypes = cqlTableOrigin.getColumnCqlTypes();

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

                        // Get target row from PostgreSQL
                        Map<String, Object> targetRow = selectByPKStatement.getRecord(connection, r.getPk());

                        if (targetRow == null) {
                            // Missing in target
                            jobCounter.increment(JobCounter.CounterType.MISSING);
                            logger.error("Missing target row found for key: {}", r.getPk());
                            hasDiff.set(true);

                            if (autoCorrectMissing && upsertStatement != null) {
                                try {
                                    upsertStatement.addToBatch(r);
                                    if (upsertStatement.shouldExecuteBatch()) {
                                        upsertStatement.executeBatch();
                                    }
                                    jobCounter.increment(JobCounter.CounterType.CORRECTED_MISSING);
                                    logger.info("Inserted missing row in target: {}", r.getPk());
                                } catch (SQLException e) {
                                    logger.error("Failed to insert missing row: {}", e.getMessage());
                                }
                            }
                        } else {
                            // Compare values
                            String diffData = compareRows(originRow, targetRow, r.getPk());
                            if (!diffData.isEmpty()) {
                                jobCounter.increment(JobCounter.CounterType.MISMATCH);
                                logger.error("Mismatch row found for key: {} Mismatch: {}", r.getPk(), diffData);
                                hasDiff.set(true);

                                if (autoCorrectMismatch && upsertStatement != null) {
                                    try {
                                        upsertStatement.addToBatch(r);
                                        if (upsertStatement.shouldExecuteBatch()) {
                                            upsertStatement.executeBatch();
                                        }
                                        jobCounter.increment(JobCounter.CounterType.CORRECTED_MISMATCH);
                                        logger.info("Corrected mismatch row in target: {}", r.getPk());
                                    } catch (SQLException e) {
                                        logger.error("Failed to correct mismatch row: {}", e.getMessage());
                                    }
                                }
                            } else {
                                jobCounter.increment(JobCounter.CounterType.VALID);
                            }
                        }
                    }
                }

                // Flush any remaining batch
                if (upsertStatement != null && upsertStatement.getCurrentBatchCount() > 0) {
                    upsertStatement.executeBatch();
                    upsertStatement.commit();
                }

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

    /**
     * Compares an origin Cassandra row with a PostgreSQL target row.
     *
     * @param originRow
     *            the Cassandra row
     * @param targetRow
     *            the PostgreSQL row as a map
     * @param pk
     *            the primary key for logging
     *
     * @return a string describing the differences, or empty if equal
     */
    private String compareRows(Row originRow, Map<String, Object> targetRow, Object pk) {
        StringBuilder diffData = new StringBuilder();

        for (String targetColName : targetColumnNames) {
            // Find corresponding origin column
            int originIndex = -1;
            for (int i = 0; i < originColumnNames.size(); i++) {
                if (originColumnNames.get(i).equalsIgnoreCase(targetColName)) {
                    originIndex = i;
                    break;
                }
            }

            if (originIndex < 0) {
                // Column doesn't exist in origin, skip
                continue;
            }

            try {
                Object originValue = originSession.getCqlTable().getData(originIndex, originRow);
                Object targetValue = targetRow.get(targetColName);

                // Convert origin value to PostgreSQL-compatible type for comparison
                DataType originType = originColumnTypes.get(originIndex);
                Object convertedOriginValue = null;
                if (originValue != null) {
                    try {
                        // Convert for comparison - don't need connection for simple comparisons
                        convertedOriginValue = convertForComparison(originValue, originType);
                    } catch (Exception e) {
                        convertedOriginValue = originValue;
                    }
                }

                // Compare values
                if (!valuesEqual(convertedOriginValue, targetValue)) {
                    diffData.append("Column:").append(targetColName).append("-origin[").append(originValue)
                            .append("]-target[").append(targetValue).append("]; ");
                }
            } catch (Exception e) {
                diffData.append("Column:").append(targetColName).append(" Exception comparing values: ")
                        .append(e.getMessage()).append("; ");
            }
        }

        return diffData.toString();
    }

    /**
     * Converts a Cassandra value for comparison with PostgreSQL value.
     */
    private Object convertForComparison(Object value, DataType type) {
        if (value == null) {
            return null;
        }

        // Handle common type conversions for comparison
        if (value instanceof java.time.Instant) {
            return typeMapper.convertTimestamp((java.time.Instant) value);
        }
        if (value instanceof java.nio.ByteBuffer) {
            return typeMapper.convertBlob((java.nio.ByteBuffer) value);
        }
        if (value instanceof java.math.BigInteger) {
            return new java.math.BigDecimal((java.math.BigInteger) value);
        }

        return value;
    }

    /**
     * Compares two values for equality, handling nulls and type differences.
     */
    private boolean valuesEqual(Object origin, Object target) {
        if (origin == null && target == null) {
            return true;
        }
        if (origin == null || target == null) {
            return false;
        }

        // Handle byte array comparison
        if (origin instanceof byte[] && target instanceof byte[]) {
            return java.util.Arrays.equals((byte[]) origin, (byte[]) target);
        }

        // Handle numeric comparison with tolerance
        if (origin instanceof Number && target instanceof Number) {
            double originVal = ((Number) origin).doubleValue();
            double targetVal = ((Number) target).doubleValue();
            return Math.abs(originVal - targetVal) < 0.0001;
        }

        // String comparison
        return Objects.equals(origin.toString(), target.toString());
    }

    /**
     * Gets the PostgreSQL connection factory.
     */
    public PostgresConnectionFactory getPostgresConnectionFactory() {
        return postgresConnectionFactory;
    }
}
