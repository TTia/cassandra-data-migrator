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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.datastax.cdm.properties.IPropertyHelper;
import com.datastax.cdm.properties.KnownProperties;
import com.datastax.cdm.schema.CqlTable;
import com.datastax.cdm.schema.PostgresTable;
import com.datastax.cdm.schema.PostgresTypeMapper;
import com.datastax.oss.driver.api.core.type.DataTypes;

public class PostgresUpsertStatementTest {

    @Mock
    private PostgresTable targetTable;

    @Mock
    private CqlTable originTable;

    @Mock
    private PostgresTypeMapper typeMapper;

    @Mock
    private IPropertyHelper propertyHelper;

    @Mock
    private Connection connection;

    @Mock
    private PreparedStatement preparedStatement;

    @BeforeEach
    public void setup() throws SQLException {
        MockitoAnnotations.openMocks(this);

        // Setup target table
        when(targetTable.getColumnNames()).thenReturn(Arrays.asList("id", "name", "value"));
        when(targetTable.getQualifiedTableName()).thenReturn("public.test_table");
        when(targetTable.getPrimaryKeyColumns()).thenReturn(Collections.singletonList("id"));

        // Setup origin table
        when(originTable.getColumnNames(false)).thenReturn(Arrays.asList("id", "name", "value"));
        when(originTable.getColumnCqlTypes()).thenReturn(Arrays.asList(
                DataTypes.UUID, DataTypes.TEXT, DataTypes.INT));

        // Setup property helper with default values
        when(propertyHelper.getString(KnownProperties.PG_UPSERT_MODE)).thenReturn("upsert");
        when(propertyHelper.getInteger(KnownProperties.PG_BATCH_SIZE)).thenReturn(100);
        when(propertyHelper.getInteger(KnownProperties.PG_TRANSACTION_SIZE)).thenReturn(500);
        when(propertyHelper.getStringList(KnownProperties.PG_ON_CONFLICT_COLUMNS)).thenReturn(null);

        // Setup connection mock
        when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
        when(connection.getAutoCommit()).thenReturn(true);
    }

    @Test
    public void constructor_createsInstance() {
        PostgresUpsertStatement statement = new PostgresUpsertStatement(
                targetTable, originTable, typeMapper, propertyHelper);

        assertNotNull(statement);
        assertNotNull(statement.getInsertStatement());
        assertNotNull(statement.getUpsertStatement());
    }

    @Test
    public void buildInsertStatement_correctFormat() {
        PostgresUpsertStatement statement = new PostgresUpsertStatement(
                targetTable, originTable, typeMapper, propertyHelper);

        String sql = statement.getInsertStatement();

        assertTrue(sql.startsWith("INSERT INTO public.test_table"));
        assertTrue(sql.contains("\"id\""));
        assertTrue(sql.contains("\"name\""));
        assertTrue(sql.contains("\"value\""));
        assertTrue(sql.contains("VALUES (?, ?, ?)"));
    }

    @Test
    public void buildUpsertStatement_correctFormat() {
        PostgresUpsertStatement statement = new PostgresUpsertStatement(
                targetTable, originTable, typeMapper, propertyHelper);

        String sql = statement.getUpsertStatement();

        assertTrue(sql.contains("INSERT INTO public.test_table"));
        assertTrue(sql.contains("ON CONFLICT (\"id\")"));
        assertTrue(sql.contains("DO UPDATE SET"));
        assertTrue(sql.contains("\"name\" = EXCLUDED.\"name\""));
        assertTrue(sql.contains("\"value\" = EXCLUDED.\"value\""));
    }

    @Test
    public void buildUpsertStatement_withMultipleConflictColumns() {
        when(targetTable.getPrimaryKeyColumns()).thenReturn(Arrays.asList("id", "name"));
        when(propertyHelper.getStringList(KnownProperties.PG_ON_CONFLICT_COLUMNS))
                .thenReturn(Arrays.asList("id", "name"));

        PostgresUpsertStatement statement = new PostgresUpsertStatement(
                targetTable, originTable, typeMapper, propertyHelper);

        String sql = statement.getUpsertStatement();

        assertTrue(sql.contains("ON CONFLICT (\"id\", \"name\")"));
        assertTrue(sql.contains("\"value\" = EXCLUDED.\"value\""));
        assertFalse(sql.contains("\"id\" = EXCLUDED"));
        assertFalse(sql.contains("\"name\" = EXCLUDED.\"name\""));
    }

    @Test
    public void buildUpsertStatement_allColumnsAreKeys_usesDoNothing() {
        when(targetTable.getColumnNames()).thenReturn(Collections.singletonList("id"));
        when(targetTable.getPrimaryKeyColumns()).thenReturn(Collections.singletonList("id"));
        when(originTable.getColumnNames(false)).thenReturn(Collections.singletonList("id"));
        when(originTable.getColumnCqlTypes()).thenReturn(Collections.singletonList(DataTypes.UUID));

        PostgresUpsertStatement statement = new PostgresUpsertStatement(
                targetTable, originTable, typeMapper, propertyHelper);

        String sql = statement.getUpsertStatement();

        assertTrue(sql.contains("DO NOTHING"));
        assertFalse(sql.contains("DO UPDATE SET"));
    }

    @Test
    public void initialize_setsUpConnection() throws SQLException {
        PostgresUpsertStatement statement = new PostgresUpsertStatement(
                targetTable, originTable, typeMapper, propertyHelper);

        statement.initialize(connection);

        verify(connection).setAutoCommit(false);
        verify(connection).prepareStatement(anyString());
    }

    @Test
    public void initialize_insertMode_usesInsertStatement() throws SQLException {
        when(propertyHelper.getString(KnownProperties.PG_UPSERT_MODE)).thenReturn("insert");

        PostgresUpsertStatement statement = new PostgresUpsertStatement(
                targetTable, originTable, typeMapper, propertyHelper);

        statement.initialize(connection);

        verify(connection).prepareStatement(statement.getInsertStatement());
    }

    @Test
    public void addToBatch_withoutInitialize_throwsException() {
        PostgresUpsertStatement statement = new PostgresUpsertStatement(
                targetTable, originTable, typeMapper, propertyHelper);

        assertThrows(IllegalStateException.class, () -> statement.addToBatch(null));
    }

    @Test
    public void getCurrentBatchCount_initiallyZero() {
        PostgresUpsertStatement statement = new PostgresUpsertStatement(
                targetTable, originTable, typeMapper, propertyHelper);

        assertEquals(0, statement.getCurrentBatchCount());
    }

    @Test
    public void shouldExecuteBatch_basedOnBatchSize() {
        when(propertyHelper.getInteger(KnownProperties.PG_BATCH_SIZE)).thenReturn(5);

        PostgresUpsertStatement statement = new PostgresUpsertStatement(
                targetTable, originTable, typeMapper, propertyHelper);

        assertFalse(statement.shouldExecuteBatch()); // count is 0
    }

    @Test
    public void executeBatch_emptyBatch_returnsEmptyArray() throws SQLException {
        PostgresUpsertStatement statement = new PostgresUpsertStatement(
                targetTable, originTable, typeMapper, propertyHelper);
        statement.initialize(connection);

        int[] results = statement.executeBatch();

        assertEquals(0, results.length);
    }

    @Test
    public void clearBatch_clearsCount() throws SQLException {
        PostgresUpsertStatement statement = new PostgresUpsertStatement(
                targetTable, originTable, typeMapper, propertyHelper);
        statement.initialize(connection);
        statement.clearBatch();

        assertEquals(0, statement.getCurrentBatchCount());
    }

    @Test
    public void commit_commitsTransaction() throws SQLException {
        PostgresUpsertStatement statement = new PostgresUpsertStatement(
                targetTable, originTable, typeMapper, propertyHelper);
        statement.initialize(connection);

        statement.commit();

        verify(connection).commit();
    }

    @Test
    public void rollback_rollsBackTransaction() throws SQLException {
        PostgresUpsertStatement statement = new PostgresUpsertStatement(
                targetTable, originTable, typeMapper, propertyHelper);
        statement.initialize(connection);

        statement.rollback();

        verify(connection).rollback();
    }

    @Test
    public void close_closesStatement() throws SQLException {
        PostgresUpsertStatement statement = new PostgresUpsertStatement(
                targetTable, originTable, typeMapper, propertyHelper);
        statement.initialize(connection);

        statement.close();

        verify(preparedStatement).close();
    }

    @Test
    public void close_restoresAutoCommit() throws SQLException {
        when(connection.getAutoCommit()).thenReturn(true);

        PostgresUpsertStatement statement = new PostgresUpsertStatement(
                targetTable, originTable, typeMapper, propertyHelper);
        statement.initialize(connection);
        statement.close();

        verify(connection, atLeast(1)).setAutoCommit(true);
    }

    @Test
    public void setAutoCommit_setsOnConnection() throws SQLException {
        PostgresUpsertStatement statement = new PostgresUpsertStatement(
                targetTable, originTable, typeMapper, propertyHelper);
        statement.initialize(connection);

        statement.setAutoCommit(true);

        verify(connection, atLeastOnce()).setAutoCommit(true);
    }

    // ========== Error Code Tests ==========

    @Test
    public void postgresErrorCode_isConstraintViolation() {
        assertTrue(PostgresUpsertStatement.PostgresErrorCode.isConstraintViolation("23505"));
        assertFalse(PostgresUpsertStatement.PostgresErrorCode.isConstraintViolation("23503"));
        assertFalse(PostgresUpsertStatement.PostgresErrorCode.isConstraintViolation(null));
    }

    @Test
    public void postgresErrorCode_isFatalError() {
        assertTrue(PostgresUpsertStatement.PostgresErrorCode.isFatalError("53100"));
        assertFalse(PostgresUpsertStatement.PostgresErrorCode.isFatalError("23505"));
        assertFalse(PostgresUpsertStatement.PostgresErrorCode.isFatalError(null));
    }

    @Test
    public void postgresErrorCode_isConnectionError() {
        assertTrue(PostgresUpsertStatement.PostgresErrorCode.isConnectionError("08000"));
        assertTrue(PostgresUpsertStatement.PostgresErrorCode.isConnectionError("08003"));
        assertTrue(PostgresUpsertStatement.PostgresErrorCode.isConnectionError("08006"));
        assertFalse(PostgresUpsertStatement.PostgresErrorCode.isConnectionError("23505"));
        assertFalse(PostgresUpsertStatement.PostgresErrorCode.isConnectionError(null));
    }

    // ========== Configuration Tests ==========

    @Test
    public void constructor_withNullBatchSize_usesDefault() {
        when(propertyHelper.getInteger(KnownProperties.PG_BATCH_SIZE)).thenReturn(null);

        PostgresUpsertStatement statement = new PostgresUpsertStatement(
                targetTable, originTable, typeMapper, propertyHelper);

        assertNotNull(statement);
    }

    @Test
    public void constructor_withNullTransactionSize_usesDefault() {
        when(propertyHelper.getInteger(KnownProperties.PG_TRANSACTION_SIZE)).thenReturn(null);

        PostgresUpsertStatement statement = new PostgresUpsertStatement(
                targetTable, originTable, typeMapper, propertyHelper);

        assertNotNull(statement);
    }

    @Test
    public void constructor_withCustomConflictColumns() {
        List<String> conflictCols = Arrays.asList("id", "name");
        when(propertyHelper.getStringList(KnownProperties.PG_ON_CONFLICT_COLUMNS)).thenReturn(conflictCols);
        when(targetTable.getPrimaryKeyColumns()).thenReturn(Collections.singletonList("id"));

        PostgresUpsertStatement statement = new PostgresUpsertStatement(
                targetTable, originTable, typeMapper, propertyHelper);

        String sql = statement.getUpsertStatement();
        assertTrue(sql.contains("ON CONFLICT (\"id\", \"name\")"));
    }

    @Test
    public void quoteIdentifier_handlesReservedWords() {
        // Test that identifiers are properly quoted
        when(targetTable.getColumnNames()).thenReturn(Arrays.asList("select", "from", "table"));
        when(originTable.getColumnNames(false)).thenReturn(Arrays.asList("select", "from", "table"));
        when(originTable.getColumnCqlTypes()).thenReturn(Arrays.asList(
                DataTypes.TEXT, DataTypes.TEXT, DataTypes.TEXT));
        when(targetTable.getPrimaryKeyColumns()).thenReturn(Collections.singletonList("select"));

        PostgresUpsertStatement statement = new PostgresUpsertStatement(
                targetTable, originTable, typeMapper, propertyHelper);

        String sql = statement.getInsertStatement();
        assertTrue(sql.contains("\"select\""));
        assertTrue(sql.contains("\"from\""));
        assertTrue(sql.contains("\"table\""));
    }
}
