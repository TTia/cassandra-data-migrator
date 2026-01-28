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
package com.datastax.cdm.cql.statement;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.cdm.data.EnhancedPK;
import com.datastax.cdm.data.Record;
import com.datastax.cdm.properties.IPropertyHelper;
import com.datastax.cdm.properties.KnownProperties;
import com.datastax.cdm.schema.CqlTable;
import com.datastax.cdm.schema.PostgresTable;
import com.datastax.cdm.schema.PostgresTypeMapper;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.type.DataType;

/**
 * Builds and executes PostgreSQL INSERT/UPSERT statements for migrating data from Cassandra.
 * Supports batch execution, transaction management, and retry logic.
 */
public class PostgresUpsertStatement {

    private static final Logger logger = LoggerFactory.getLogger(PostgresUpsertStatement.class);
    private static final int DEFAULT_BATCH_SIZE = 1000;
    private static final int DEFAULT_RETRY_ATTEMPTS = 3;
    private static final long INITIAL_RETRY_DELAY_MS = 100;

    private final PostgresTable targetTable;
    private final CqlTable originTable;
    private final PostgresTypeMapper typeMapper;
    private final IPropertyHelper propertyHelper;

    private final String insertStatement;
    private final String upsertStatement;
    private final boolean useUpsert;
    private final int batchSize;
    private final int transactionSize;
    private final List<String> onConflictColumns;

    // Column mapping
    private final List<String> targetColumnNames;
    private final List<DataType> originColumnTypes;
    private final List<Integer> columnMapping; // origin index -> target column position

    // Batch state
    private Connection connection;
    private PreparedStatement preparedStatement;
    private int currentBatchCount = 0;
    private int totalRecordsInTransaction = 0;
    private boolean autoCommit = false;

    /**
     * Creates a PostgresUpsertStatement for the given tables.
     *
     * @param targetTable    the PostgreSQL target table
     * @param originTable    the Cassandra origin table
     * @param typeMapper     the type mapper for value conversion
     * @param propertyHelper the property helper for configuration
     */
    public PostgresUpsertStatement(PostgresTable targetTable, CqlTable originTable,
            PostgresTypeMapper typeMapper, IPropertyHelper propertyHelper) {
        this.targetTable = targetTable;
        this.originTable = originTable;
        this.typeMapper = typeMapper;
        this.propertyHelper = propertyHelper;

        // Load configuration
        String upsertMode = propertyHelper.getString(KnownProperties.PG_UPSERT_MODE);
        this.useUpsert = "upsert".equalsIgnoreCase(upsertMode);

        Integer configBatchSize = propertyHelper.getInteger(KnownProperties.PG_BATCH_SIZE);
        this.batchSize = configBatchSize != null ? configBatchSize : DEFAULT_BATCH_SIZE;

        Integer configTransactionSize = propertyHelper.getInteger(KnownProperties.PG_TRANSACTION_SIZE);
        this.transactionSize = configTransactionSize != null ? configTransactionSize : batchSize * 5;

        List<String> conflictCols = propertyHelper.getStringList(KnownProperties.PG_ON_CONFLICT_COLUMNS);
        this.onConflictColumns = conflictCols != null ? conflictCols : targetTable.getPrimaryKeyColumns();

        // Build column mapping
        this.targetColumnNames = new ArrayList<>(targetTable.getColumnNames());
        this.originColumnTypes = originTable.getColumnCqlTypes();
        this.columnMapping = buildColumnMapping();

        // Build SQL statements
        this.insertStatement = buildInsertStatement();
        this.upsertStatement = buildUpsertStatement();

        logger.info("PostgresUpsertStatement created: useUpsert={}, batchSize={}, transactionSize={}",
                useUpsert, batchSize, transactionSize);
    }

    /**
     * Initializes the statement with a database connection.
     *
     * @param connection the database connection
     * @throws SQLException if initialization fails
     */
    public void initialize(Connection connection) throws SQLException {
        this.connection = connection;
        this.autoCommit = connection.getAutoCommit();
        connection.setAutoCommit(false);

        String sql = useUpsert ? upsertStatement : insertStatement;
        this.preparedStatement = connection.prepareStatement(sql);

        logger.debug("Initialized with SQL: {}", sql);
    }

    /**
     * Binds a record to the prepared statement and adds it to the batch.
     *
     * @param record the record to bind
     * @throws SQLException if binding fails
     */
    public void addToBatch(Record record) throws SQLException {
        if (preparedStatement == null) {
            throw new IllegalStateException("Statement not initialized. Call initialize() first.");
        }

        Row originRow = record.getOriginRow();
        EnhancedPK pk = record.getPk();

        bindRecord(originRow, pk);
        preparedStatement.addBatch();
        currentBatchCount++;
        totalRecordsInTransaction++;
    }

    /**
     * Executes the current batch of statements.
     *
     * @return array of update counts
     * @throws SQLException if execution fails
     */
    public int[] executeBatch() throws SQLException {
        if (currentBatchCount == 0) {
            return new int[0];
        }

        int[] results = executeBatchWithRetry();
        preparedStatement.clearBatch();
        currentBatchCount = 0;

        return results;
    }

    /**
     * Clears the current batch without executing.
     *
     * @throws SQLException if clearing fails
     */
    public void clearBatch() throws SQLException {
        if (preparedStatement != null) {
            preparedStatement.clearBatch();
        }
        currentBatchCount = 0;
    }

    /**
     * Commits the current transaction.
     *
     * @throws SQLException if commit fails
     */
    public void commit() throws SQLException {
        if (connection != null && !connection.getAutoCommit()) {
            connection.commit();
            totalRecordsInTransaction = 0;
            logger.debug("Transaction committed");
        }
    }

    /**
     * Rolls back the current transaction.
     *
     * @throws SQLException if rollback fails
     */
    public void rollback() throws SQLException {
        if (connection != null && !connection.getAutoCommit()) {
            connection.rollback();
            totalRecordsInTransaction = 0;
            logger.debug("Transaction rolled back");
        }
    }

    /**
     * Checks if the batch should be executed based on batch size.
     *
     * @return true if batch should be executed
     */
    public boolean shouldExecuteBatch() {
        return currentBatchCount >= batchSize;
    }

    /**
     * Checks if the transaction should be committed based on transaction size.
     *
     * @return true if transaction should be committed
     */
    public boolean shouldCommit() {
        return totalRecordsInTransaction >= transactionSize;
    }

    /**
     * Sets auto-commit mode.
     *
     * @param autoCommit the auto-commit setting
     * @throws SQLException if setting fails
     */
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        this.autoCommit = autoCommit;
        if (connection != null) {
            connection.setAutoCommit(autoCommit);
        }
    }

    /**
     * Returns the current batch count.
     *
     * @return the number of records in the current batch
     */
    public int getCurrentBatchCount() {
        return currentBatchCount;
    }

    /**
     * Returns the INSERT SQL statement.
     *
     * @return the INSERT statement
     */
    public String getInsertStatement() {
        return insertStatement;
    }

    /**
     * Returns the UPSERT SQL statement.
     *
     * @return the UPSERT statement
     */
    public String getUpsertStatement() {
        return upsertStatement;
    }

    /**
     * Closes the prepared statement.
     *
     * @throws SQLException if closing fails
     */
    public void close() throws SQLException {
        if (preparedStatement != null) {
            preparedStatement.close();
            preparedStatement = null;
        }
        if (connection != null) {
            connection.setAutoCommit(autoCommit);
        }
    }

    // ==================== Private Methods ====================

    private List<Integer> buildColumnMapping() {
        List<String> originColNames = originTable.getColumnNames(false);
        List<Integer> mapping = new ArrayList<>();

        for (String targetColName : targetColumnNames) {
            int originIndex = -1;
            for (int i = 0; i < originColNames.size(); i++) {
                if (originColNames.get(i).equalsIgnoreCase(targetColName)) {
                    originIndex = i;
                    break;
                }
            }
            mapping.add(originIndex);
        }

        return mapping;
    }

    private String buildInsertStatement() {
        StringBuilder sql = new StringBuilder();
        sql.append("INSERT INTO ")
                .append(targetTable.getQualifiedTableName())
                .append(" (");

        // Column list
        for (int i = 0; i < targetColumnNames.size(); i++) {
            if (i > 0) {
                sql.append(", ");
            }
            sql.append(quoteIdentifier(targetColumnNames.get(i)));
        }

        sql.append(") VALUES (");

        // Parameter placeholders
        for (int i = 0; i < targetColumnNames.size(); i++) {
            if (i > 0) {
                sql.append(", ");
            }
            sql.append("?");
        }

        sql.append(")");

        return sql.toString();
    }

    private String buildUpsertStatement() {
        StringBuilder sql = new StringBuilder(buildInsertStatement());

        // Add ON CONFLICT clause
        sql.append(" ON CONFLICT (");
        for (int i = 0; i < onConflictColumns.size(); i++) {
            if (i > 0) {
                sql.append(", ");
            }
            sql.append(quoteIdentifier(onConflictColumns.get(i)));
        }
        sql.append(") DO UPDATE SET ");

        // Build SET clause for non-key columns
        boolean first = true;
        for (String colName : targetColumnNames) {
            if (!onConflictColumns.contains(colName)) {
                if (!first) {
                    sql.append(", ");
                }
                String quotedName = quoteIdentifier(colName);
                sql.append(quotedName).append(" = EXCLUDED.").append(quotedName);
                first = false;
            }
        }

        // If all columns are key columns, use DO NOTHING
        if (first) {
            // Remove " DO UPDATE SET " and replace with " DO NOTHING"
            int doUpdateIdx = sql.indexOf(" DO UPDATE SET ");
            sql.delete(doUpdateIdx, sql.length());
            sql.append(" DO NOTHING");
        }

        return sql.toString();
    }

    private void bindRecord(Row originRow, EnhancedPK pk) throws SQLException {
        int paramIndex = 1;

        for (int targetIdx = 0; targetIdx < targetColumnNames.size(); targetIdx++) {
            int originIdx = columnMapping.get(targetIdx);
            Object value = null;

            if (originIdx >= 0 && originIdx < originColumnTypes.size()) {
                DataType originType = originColumnTypes.get(originIdx);

                // Get value from origin row
                try {
                    value = originTable.getAndConvertData(originIdx, originRow);
                } catch (Exception e) {
                    logger.warn("Failed to get data for column {} at index {}: {}",
                            targetColumnNames.get(targetIdx), originIdx, e.getMessage());
                }

                // Convert to PostgreSQL-compatible value
                if (value != null) {
                    try {
                        value = typeMapper.convertValue(value, originType, connection);
                    } catch (Exception e) {
                        logger.warn("Failed to convert value for column {}: {}",
                                targetColumnNames.get(targetIdx), e.getMessage());
                        value = null;
                    }
                }
            }

            // Bind the value
            if (value == null) {
                preparedStatement.setNull(paramIndex, java.sql.Types.NULL);
            } else {
                preparedStatement.setObject(paramIndex, value);
            }
            paramIndex++;
        }
    }

    private int[] executeBatchWithRetry() throws SQLException {
        int attempts = 0;
        SQLException lastException = null;
        long delayMs = INITIAL_RETRY_DELAY_MS;

        while (attempts < DEFAULT_RETRY_ATTEMPTS) {
            try {
                return preparedStatement.executeBatch();
            } catch (SQLException e) {
                lastException = e;
                String sqlState = e.getSQLState();

                // Check if this is a retryable error
                if (isRetryableError(sqlState)) {
                    attempts++;
                    if (attempts < DEFAULT_RETRY_ATTEMPTS) {
                        logger.warn("Batch execution failed (attempt {}/{}), retrying in {}ms: {}",
                                attempts, DEFAULT_RETRY_ATTEMPTS, delayMs, e.getMessage());
                        try {
                            Thread.sleep(delayMs);
                            delayMs *= 2; // Exponential backoff
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            throw e;
                        }

                        // Rollback and re-add batch items
                        rollback();
                    }
                } else {
                    // Non-retryable error
                    throw e;
                }
            }
        }

        // Max retries exceeded
        logger.error("Batch execution failed after {} attempts", DEFAULT_RETRY_ATTEMPTS);
        throw lastException;
    }

    private boolean isRetryableError(String sqlState) {
        if (sqlState == null) {
            return false;
        }

        // Connection errors (Class 08)
        if (sqlState.startsWith("08")) {
            return true;
        }

        // Transaction rollback errors (Class 40)
        if (sqlState.startsWith("40")) {
            return true;
        }

        // Serialization failure
        if ("40001".equals(sqlState)) {
            return true;
        }

        // Deadlock detected
        if ("40P01".equals(sqlState)) {
            return true;
        }

        return false;
    }

    private String quoteIdentifier(String identifier) {
        // Use double quotes for PostgreSQL identifiers to preserve case
        // and handle reserved words
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    /**
     * PostgreSQL error codes for error handling.
     */
    public static class PostgresErrorCode {
        public static final String UNIQUE_VIOLATION = "23505";
        public static final String FOREIGN_KEY_VIOLATION = "23503";
        public static final String NOT_NULL_VIOLATION = "23502";
        public static final String CONNECTION_EXCEPTION = "08000";
        public static final String CONNECTION_DOES_NOT_EXIST = "08003";
        public static final String CONNECTION_FAILURE = "08006";
        public static final String SERIALIZATION_FAILURE = "40001";
        public static final String DEADLOCK_DETECTED = "40P01";
        public static final String DISK_FULL = "53100";

        /**
         * Checks if the error is a constraint violation that might be resolved by upsert.
         */
        public static boolean isConstraintViolation(String sqlState) {
            return UNIQUE_VIOLATION.equals(sqlState);
        }

        /**
         * Checks if the error is fatal and should stop migration.
         */
        public static boolean isFatalError(String sqlState) {
            return DISK_FULL.equals(sqlState);
        }

        /**
         * Checks if the error is a connection error.
         */
        public static boolean isConnectionError(String sqlState) {
            return sqlState != null && sqlState.startsWith("08");
        }
    }
}
