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
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.cdm.data.Record;
import com.datastax.cdm.properties.IPropertyHelper;
import com.datastax.cdm.properties.KnownProperties;
import com.datastax.cdm.schema.CqlTable;
import com.datastax.cdm.schema.PostgresTable;
import com.datastax.cdm.schema.PostgresTypeMapper;
import com.datastax.oss.driver.api.core.cql.Row;

/**
 * Builds and executes PostgreSQL INSERT/UPSERT statements for migrating data from Cassandra. Supports batch execution
 * and transaction management. A failed batch is not retried: the caller fails the partition and CDM reruns it.
 */
public class PostgresUpsertStatement {

    private static final Logger logger = LoggerFactory.getLogger(PostgresUpsertStatement.class);
    private static final int DEFAULT_BATCH_SIZE = 1000;

    private final PostgresTable targetTable;
    private final IPropertyHelper propertyHelper;

    private final String insertStatement;
    private final String upsertStatement;
    private final boolean useUpsert;
    private final int batchSize;
    private final int transactionSize;
    private final List<String> onConflictColumns;

    private final PostgresColumnMapping mapping;
    private final List<String> columns;

    // Batch state
    private Connection connection;
    private PreparedStatement preparedStatement;
    private int currentBatchCount = 0;
    private int totalRecordsInTransaction = 0;
    private boolean autoCommit = false;

    /**
     * Creates a PostgresUpsertStatement for the given tables.
     *
     * @param targetTable
     *            the PostgreSQL target table
     * @param originTable
     *            the Cassandra origin table
     * @param typeMapper
     *            the type mapper for value conversion
     * @param propertyHelper
     *            the property helper for configuration
     */
    public PostgresUpsertStatement(PostgresTable targetTable, CqlTable originTable, PostgresTypeMapper typeMapper,
            IPropertyHelper propertyHelper) {
        this.targetTable = targetTable;
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

        this.mapping = new PostgresColumnMapping(targetTable, originTable, typeMapper);
        this.columns = mapping.getColumns();
        if (useUpsert && (onConflictColumns.isEmpty() || !columns.containsAll(onConflictColumns))) {
            throw new IllegalStateException("Cannot upsert into " + targetTable.getQualifiedTableName()
                    + ": the conflict columns " + onConflictColumns + " must be non-empty and exist in the origin"
                    + " table. Set " + KnownProperties.PG_ON_CONFLICT_COLUMNS + ", or " + KnownProperties.PG_UPSERT_MODE
                    + "=insert");
        }

        // Build SQL statements
        this.insertStatement = buildInsertStatement();
        this.upsertStatement = buildUpsertStatement();

        logger.info("PostgresUpsertStatement created: useUpsert={}, batchSize={}, transactionSize={}", useUpsert,
                batchSize, transactionSize);
    }

    /**
     * Initializes the statement with a database connection.
     *
     * @param connection
     *            the database connection
     *
     * @throws SQLException
     *             if initialization fails
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
     * @param record
     *            the record to bind
     *
     * @throws SQLException
     *             if binding fails, including a value that cannot be converted
     */
    public void addToBatch(Record record) throws SQLException {
        if (preparedStatement == null) {
            throw new IllegalStateException("Statement not initialized. Call initialize() first.");
        }

        bindRecord(record.getOriginRow());
        preparedStatement.addBatch();
        currentBatchCount++;
        totalRecordsInTransaction++;
    }

    /**
     * Executes the current batch of statements. The batch is cleared whether or not it succeeds.
     *
     * @return array of update counts
     *
     * @throws SQLException
     *             if execution fails
     */
    public int[] executeBatch() throws SQLException {
        if (currentBatchCount == 0) {
            return new int[0];
        }

        try {
            return preparedStatement.executeBatch();
        } finally {
            preparedStatement.clearBatch();
            currentBatchCount = 0;
        }
    }

    /**
     * Commits the current transaction.
     *
     * @throws SQLException
     *             if commit fails
     */
    public void commit() throws SQLException {
        if (connection != null && !connection.getAutoCommit()) {
            connection.commit();
            totalRecordsInTransaction = 0;
            logger.debug("Transaction committed");
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
     * Closes the prepared statement, rolls back anything not committed, then restores the auto-commit mode.
     *
     * @throws SQLException
     *             if closing fails
     */
    public void close() throws SQLException {
        if (preparedStatement != null) {
            preparedStatement.close();
            preparedStatement = null;
        }
        if (connection != null) {
            // Uncommitted rows at this point belong to a failed partition; restoring auto-commit would commit them
            if (!connection.getAutoCommit()) {
                connection.rollback();
            }
            connection.setAutoCommit(autoCommit);
        }
    }

    // ==================== Private Methods ====================

    private String buildInsertStatement() {
        StringBuilder sql = new StringBuilder();
        sql.append("INSERT INTO ").append(targetTable.getQualifiedTableName()).append(" (");

        // Column list
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                sql.append(", ");
            }
            sql.append(PostgresTable.quoteIdentifier(columns.get(i)));
        }

        sql.append(") VALUES (");

        // Parameter placeholders
        for (int i = 0; i < columns.size(); i++) {
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
            sql.append(PostgresTable.quoteIdentifier(onConflictColumns.get(i)));
        }
        sql.append(") DO UPDATE SET ");

        // Build SET clause for non-key columns
        boolean first = true;
        for (String colName : columns) {
            if (!onConflictColumns.contains(colName)) {
                if (!first) {
                    sql.append(", ");
                }
                String quotedName = PostgresTable.quoteIdentifier(colName);
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

    private void bindRecord(Row originRow) throws SQLException {
        for (int i = 0; i < columns.size(); i++) {
            Object value = mapping.value(i, originRow, connection);
            if (value == null) {
                preparedStatement.setNull(i + 1, java.sql.Types.NULL);
            } else {
                preparedStatement.setObject(i + 1, value);
            }
        }
    }
}
