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
import static org.mockito.Mockito.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.datastax.cdm.data.EnhancedPK;
import com.datastax.cdm.schema.CqlTable;
import com.datastax.cdm.schema.PostgresTable;

public class PostgresSelectByPKStatementTest {

    @Mock
    private PostgresTable targetTable;

    @Mock
    private CqlTable originTable;

    @Mock
    private Connection connection;

    @Mock
    private PreparedStatement preparedStatement;

    @Mock
    private ResultSet resultSet;

    @Mock
    private EnhancedPK pk;

    @BeforeEach
    public void setup() throws SQLException {
        MockitoAnnotations.openMocks(this);

        // Setup target table
        when(targetTable.getColumnNames()).thenReturn(Arrays.asList("id", "name", "value"));
        when(targetTable.getQualifiedTableName()).thenReturn("public.test_table");
        when(targetTable.getPrimaryKeyColumns()).thenReturn(Collections.singletonList("id"));

        // Setup origin table
        when(originTable.getPartitionKeyNames(false)).thenReturn(Collections.singletonList("id"));

        // Setup connection mock
        when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
    }

    @Test
    public void constructor_createsInstance() {
        PostgresSelectByPKStatement statement = new PostgresSelectByPKStatement(targetTable, originTable);

        assertNotNull(statement);
        assertNotNull(statement.getSelectStatement());
        assertEquals(Collections.singletonList("id"), statement.getPrimaryKeyColumns());
    }

    @Test
    public void buildSelectStatement_correctFormat() {
        PostgresSelectByPKStatement statement = new PostgresSelectByPKStatement(targetTable, originTable);

        String sql = statement.getSelectStatement();

        assertTrue(sql.startsWith("SELECT"));
        assertTrue(sql.contains("\"id\""));
        assertTrue(sql.contains("\"name\""));
        assertTrue(sql.contains("\"value\""));
        assertTrue(sql.contains("FROM public.test_table"));
        assertTrue(sql.contains("WHERE \"id\" = ?"));
    }

    @Test
    public void buildSelectStatement_withMultiplePKColumns() {
        when(targetTable.getPrimaryKeyColumns()).thenReturn(Arrays.asList("id", "partition_id"));

        PostgresSelectByPKStatement statement = new PostgresSelectByPKStatement(targetTable, originTable);

        String sql = statement.getSelectStatement();

        assertTrue(sql.contains("WHERE \"id\" = ? AND \"partition_id\" = ?"));
    }

    @Test
    public void getRecord_notFound_returnsNull() throws SQLException {
        when(resultSet.next()).thenReturn(false);

        UUID testId = UUID.randomUUID();
        when(pk.getPKValues()).thenReturn(Collections.singletonList(testId));

        PostgresSelectByPKStatement statement = new PostgresSelectByPKStatement(targetTable, originTable);
        Map<String, Object> result = statement.getRecord(connection, pk);

        assertNull(result);
    }

    @Test
    public void getRecord_found_returnsRow() throws SQLException {
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getObject(1)).thenReturn(UUID.randomUUID());
        when(resultSet.getObject(2)).thenReturn("test_name");
        when(resultSet.getObject(3)).thenReturn(42);

        UUID testId = UUID.randomUUID();
        when(pk.getPKValues()).thenReturn(Collections.singletonList(testId));

        PostgresSelectByPKStatement statement = new PostgresSelectByPKStatement(targetTable, originTable);
        Map<String, Object> result = statement.getRecord(connection, pk);

        assertNotNull(result);
        assertEquals(3, result.size());
        assertTrue(result.containsKey("id"));
        assertTrue(result.containsKey("name"));
        assertTrue(result.containsKey("value"));
        assertEquals("test_name", result.get("name"));
        assertEquals(42, result.get("value"));
    }

    @Test
    public void recordExists_notFound_returnsFalse() throws SQLException {
        when(resultSet.next()).thenReturn(false);

        UUID testId = UUID.randomUUID();
        when(pk.getPKValues()).thenReturn(Collections.singletonList(testId));

        PostgresSelectByPKStatement statement = new PostgresSelectByPKStatement(targetTable, originTable);
        boolean result = statement.recordExists(connection, pk);

        assertFalse(result);
    }

    @Test
    public void recordExists_found_returnsTrue() throws SQLException {
        when(resultSet.next()).thenReturn(true);

        UUID testId = UUID.randomUUID();
        when(pk.getPKValues()).thenReturn(Collections.singletonList(testId));

        PostgresSelectByPKStatement statement = new PostgresSelectByPKStatement(targetTable, originTable);
        boolean result = statement.recordExists(connection, pk);

        assertTrue(result);
    }

    @Test
    public void getPrimaryKeyColumns_returnsConfiguredColumns() {
        List<String> pkCols = Arrays.asList("id", "tenant_id");
        when(targetTable.getPrimaryKeyColumns()).thenReturn(pkCols);

        PostgresSelectByPKStatement statement = new PostgresSelectByPKStatement(targetTable, originTable);

        assertEquals(pkCols, statement.getPrimaryKeyColumns());
    }
}
