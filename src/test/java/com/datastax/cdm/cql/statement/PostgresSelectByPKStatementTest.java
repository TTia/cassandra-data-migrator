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
import java.sql.Types;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.datastax.cdm.data.EnhancedPK;
import com.datastax.cdm.data.Record;
import com.datastax.cdm.schema.CqlTable;
import com.datastax.cdm.schema.PostgresTable;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.type.DataTypes;

public class PostgresSelectByPKStatementTest {

    private static final UUID ID = UUID.randomUUID();

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

    @Mock
    private Record record;

    @Mock
    private Row originRow;

    @BeforeEach
    public void setup() throws SQLException {
        MockitoAnnotations.openMocks(this);

        when(targetTable.getColumnNames()).thenReturn(Arrays.asList("id", "name", "value"));
        when(targetTable.getQualifiedTableName()).thenReturn("\"public\".\"test_table\"");
        when(targetTable.getPrimaryKeyColumns()).thenReturn(Collections.singletonList("id"));

        when(originTable.getColumnNames(false)).thenReturn(Arrays.asList("id", "name", "value"));
        when(originTable.getColumnCqlTypes()).thenReturn(Arrays.asList(DataTypes.UUID, DataTypes.TEXT, DataTypes.INT));
        when(originTable.getPartitionKeyNames(false)).thenReturn(Collections.singletonList("id"));
        when(originTable.getDataType("id")).thenReturn(DataTypes.UUID);
        when(originTable.getAndConvertData(1, originRow)).thenReturn("n");
        when(originTable.getAndConvertData(2, originRow)).thenReturn(42);

        when(pk.getPKValues()).thenReturn(Collections.singletonList(ID));
        when(record.getPk()).thenReturn(pk);
        when(record.getOriginRow()).thenReturn(originRow);

        when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
    }

    @Test
    public void selectStatement_comparesEveryNonKeyColumnInPostgres() {
        PostgresSelectByPKStatement statement = new PostgresSelectByPKStatement(targetTable, originTable);

        assertEquals("SELECT \"name\", (\"name\" IS NOT DISTINCT FROM ?), \"value\", (\"value\" IS NOT DISTINCT FROM ?)"
                + " FROM \"public\".\"test_table\" WHERE \"id\" = ?", statement.getSelectStatement());
    }

    @Test
    public void selectStatement_leavesOutPostgresOnlyColumns() {
        when(targetTable.getColumnNames()).thenReturn(Arrays.asList("id", "name", "ingested_at"));

        PostgresSelectByPKStatement statement = new PostgresSelectByPKStatement(targetTable, originTable);

        assertFalse(statement.getSelectStatement().contains("ingested_at"));
    }

    @Test
    public void selectStatement_withOnlyKeyColumns_selectsOne() {
        when(targetTable.getColumnNames()).thenReturn(Collections.singletonList("id"));

        PostgresSelectByPKStatement statement = new PostgresSelectByPKStatement(targetTable, originTable);

        assertEquals("SELECT 1 FROM \"public\".\"test_table\" WHERE \"id\" = ?", statement.getSelectStatement());
    }

    @Test
    public void selectStatement_withMultiplePKColumns() {
        when(targetTable.getPrimaryKeyColumns()).thenReturn(Arrays.asList("id", "partition_id"));
        when(originTable.getPKNames(false)).thenReturn(Arrays.asList("id", "partition_id"));

        PostgresSelectByPKStatement statement = new PostgresSelectByPKStatement(targetTable, originTable);

        assertTrue(statement.getSelectStatement().endsWith("WHERE \"id\" = ? AND \"partition_id\" = ?"));
    }

    @Test
    public void constructor_withoutPrimaryKey_failsFast() {
        when(targetTable.getPrimaryKeyColumns()).thenReturn(Collections.emptyList());

        assertThrows(IllegalStateException.class, () -> new PostgresSelectByPKStatement(targetTable, originTable));
    }

    @Test
    public void constructor_withKeyColumnMissingFromOriginKey_failsFast() {
        // A PostgreSQL-generated key, such as a bigserial id, has no origin value to look the row up by
        when(targetTable.getPrimaryKeyColumns()).thenReturn(Collections.singletonList("row_id"));

        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> new PostgresSelectByPKStatement(targetTable, originTable));

        assertTrue(e.getMessage().contains("row_id"));
    }

    @Test
    public void findMismatches_rowMissing_returnsNull() throws SQLException {
        when(resultSet.next()).thenReturn(false);

        PostgresSelectByPKStatement statement = new PostgresSelectByPKStatement(targetTable, originTable);

        assertNull(statement.findMismatches(connection, record));
    }

    @Test
    public void findMismatches_rowEqual_bindsOriginValuesThenKey() throws SQLException {
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getBoolean(2)).thenReturn(true);
        when(resultSet.getBoolean(4)).thenReturn(true);

        PostgresSelectByPKStatement statement = new PostgresSelectByPKStatement(targetTable, originTable);

        assertTrue(statement.findMismatches(connection, record).isEmpty());
        verify(preparedStatement).setObject(1, "n");
        verify(preparedStatement).setObject(2, 42);
        verify(preparedStatement).setObject(3, ID);
    }

    @Test
    public void findMismatches_columnDiffers_namesItWithBothValues() throws SQLException {
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getBoolean(2)).thenReturn(true);
        when(resultSet.getBoolean(4)).thenReturn(false);
        when(resultSet.getString(3)).thenReturn("41");

        PostgresSelectByPKStatement statement = new PostgresSelectByPKStatement(targetTable, originTable);
        List<String> mismatches = statement.findMismatches(connection, record);

        assertEquals(Collections.singletonList("value origin[42] target[41]"), mismatches);
    }

    @Test
    public void findMismatches_nullOriginValue_bindsNull() throws SQLException {
        when(originTable.getAndConvertData(1, originRow)).thenReturn(null);
        when(resultSet.next()).thenReturn(false);

        new PostgresSelectByPKStatement(targetTable, originTable).findMismatches(connection, record);

        verify(preparedStatement).setNull(1, Types.NULL);
    }

    @Test
    public void findMismatches_valueThatCannotBeConverted_fails() {
        when(originTable.getAndConvertData(2, originRow)).thenThrow(new IllegalArgumentException("bad value"));

        PostgresSelectByPKStatement statement = new PostgresSelectByPKStatement(targetTable, originTable);
        SQLException e = assertThrows(SQLException.class, () -> statement.findMismatches(connection, record));

        assertTrue(e.getMessage().contains("value"));
    }

    @Test
    public void getPrimaryKeyColumns_returnsConfiguredColumns() {
        List<String> pkCols = Arrays.asList("id", "tenant_id");
        when(targetTable.getPrimaryKeyColumns()).thenReturn(pkCols);
        when(originTable.getPKNames(false)).thenReturn(pkCols);

        PostgresSelectByPKStatement statement = new PostgresSelectByPKStatement(targetTable, originTable);

        assertEquals(pkCols, statement.getPrimaryKeyColumns());
    }
}
