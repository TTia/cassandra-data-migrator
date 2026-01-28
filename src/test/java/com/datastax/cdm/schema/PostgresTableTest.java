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
package com.datastax.cdm.schema;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

import java.sql.Types;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.datastax.cdm.properties.IPropertyHelper;
import com.datastax.cdm.properties.KnownProperties;

public class PostgresTableTest {

    @Mock
    private IPropertyHelper propertyHelper;

    @BeforeEach
    public void setup() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void constructor_withExplicitSchemaAndTable() {
        PostgresTable table = new PostgresTable("test_schema", "test_table");

        assertEquals("test_schema", table.getSchemaName());
        assertEquals("test_table", table.getTableName());
        assertEquals("test_schema.test_table", table.getQualifiedTableName());
    }

    @Test
    public void constructor_withNullSchema_defaultsToPublic() {
        PostgresTable table = new PostgresTable(null, "test_table");

        assertEquals("public", table.getSchemaName());
        assertEquals("test_table", table.getTableName());
    }

    @Test
    public void constructor_fromPropertyHelper_withPgTableSet() {
        when(propertyHelper.getString(KnownProperties.PG_SCHEMA)).thenReturn("my_schema");
        when(propertyHelper.getString(KnownProperties.PG_TABLE)).thenReturn("my_table");

        PostgresTable table = new PostgresTable(propertyHelper);

        assertEquals("my_schema", table.getSchemaName());
        assertEquals("my_table", table.getTableName());
    }

    @Test
    public void constructor_fromPropertyHelper_fallbackToOriginKeyspaceTable() {
        when(propertyHelper.getString(KnownProperties.PG_SCHEMA)).thenReturn(null);
        when(propertyHelper.getString(KnownProperties.PG_TABLE)).thenReturn(null);
        when(propertyHelper.getString(KnownProperties.ORIGIN_KEYSPACE_TABLE))
                .thenReturn("origin_keyspace.origin_table");

        PostgresTable table = new PostgresTable(propertyHelper);

        assertEquals("origin_keyspace", table.getSchemaName());
        assertEquals("origin_table", table.getTableName());
    }

    @Test
    public void constructor_fromPropertyHelper_fallbackWithPgSchemaSet() {
        when(propertyHelper.getString(KnownProperties.PG_SCHEMA)).thenReturn("custom_schema");
        when(propertyHelper.getString(KnownProperties.PG_TABLE)).thenReturn(null);
        when(propertyHelper.getString(KnownProperties.ORIGIN_KEYSPACE_TABLE))
                .thenReturn("origin_keyspace.origin_table");

        PostgresTable table = new PostgresTable(propertyHelper);

        assertEquals("custom_schema", table.getSchemaName());
        assertEquals("origin_table", table.getTableName());
    }

    @Test
    public void constructor_fromPropertyHelper_originTableWithoutKeyspace() {
        when(propertyHelper.getString(KnownProperties.PG_SCHEMA)).thenReturn(null);
        when(propertyHelper.getString(KnownProperties.PG_TABLE)).thenReturn(null);
        when(propertyHelper.getString(KnownProperties.ORIGIN_KEYSPACE_TABLE)).thenReturn("just_table");

        PostgresTable table = new PostgresTable(propertyHelper);

        assertEquals("public", table.getSchemaName());
        assertEquals("just_table", table.getTableName());
    }

    @Test
    public void constructor_fromPropertyHelper_noTableAvailable_throwsException() {
        when(propertyHelper.getString(KnownProperties.PG_SCHEMA)).thenReturn(null);
        when(propertyHelper.getString(KnownProperties.PG_TABLE)).thenReturn(null);
        when(propertyHelper.getString(KnownProperties.ORIGIN_KEYSPACE_TABLE)).thenReturn(null);

        assertThrows(IllegalArgumentException.class, () -> new PostgresTable(propertyHelper));
    }

    @Test
    public void isMetadataLoaded_initiallyFalse() {
        PostgresTable table = new PostgresTable("schema", "table");

        assertFalse(table.isMetadataLoaded());
    }

    @Test
    public void getColumns_beforeMetadataLoad_returnsEmptyList() {
        PostgresTable table = new PostgresTable("schema", "table");

        assertTrue(table.getColumns().isEmpty());
        assertTrue(table.getColumnNames().isEmpty());
        assertTrue(table.getPrimaryKeyColumns().isEmpty());
    }

    @Test
    public void getColumnIndex_beforeMetadataLoad_returnsNegative() {
        PostgresTable table = new PostgresTable("schema", "table");

        assertEquals(-1, table.getColumnIndex("any_column"));
    }

    @Test
    public void getSqlType_invalidIndex_throwsException() {
        PostgresTable table = new PostgresTable("schema", "table");

        assertThrows(IndexOutOfBoundsException.class, () -> table.getSqlType(0));
        assertThrows(IndexOutOfBoundsException.class, () -> table.getSqlType(-1));
    }

    @Test
    public void getSqlType_byName_notFound_throwsException() {
        PostgresTable table = new PostgresTable("schema", "table");

        assertThrows(IllegalArgumentException.class, () -> table.getSqlType("unknown_column"));
    }

    @Test
    public void columnInfo_mapDataTypeToSqlType() {
        // Test various data type mappings
        assertEquals(Types.VARCHAR, new PostgresTable.ColumnInfo("col", "text", null, true, null, 1).getSqlType());
        assertEquals(Types.VARCHAR,
                new PostgresTable.ColumnInfo("col", "character varying", null, true, null, 1).getSqlType());
        assertEquals(Types.INTEGER, new PostgresTable.ColumnInfo("col", "integer", null, true, null, 1).getSqlType());
        assertEquals(Types.BIGINT, new PostgresTable.ColumnInfo("col", "bigint", null, true, null, 1).getSqlType());
        assertEquals(Types.SMALLINT, new PostgresTable.ColumnInfo("col", "smallint", null, true, null, 1).getSqlType());
        assertEquals(Types.REAL, new PostgresTable.ColumnInfo("col", "real", null, true, null, 1).getSqlType());
        assertEquals(Types.DOUBLE,
                new PostgresTable.ColumnInfo("col", "double precision", null, true, null, 1).getSqlType());
        assertEquals(Types.NUMERIC, new PostgresTable.ColumnInfo("col", "numeric", null, true, null, 1).getSqlType());
        assertEquals(Types.BOOLEAN, new PostgresTable.ColumnInfo("col", "boolean", null, true, null, 1).getSqlType());
        assertEquals(Types.BINARY, new PostgresTable.ColumnInfo("col", "bytea", null, true, null, 1).getSqlType());
        assertEquals(Types.DATE, new PostgresTable.ColumnInfo("col", "date", null, true, null, 1).getSqlType());
        assertEquals(Types.TIME, new PostgresTable.ColumnInfo("col", "time", null, true, null, 1).getSqlType());
        assertEquals(Types.TIMESTAMP,
                new PostgresTable.ColumnInfo("col", "timestamp", null, true, null, 1).getSqlType());
        assertEquals(Types.TIMESTAMP_WITH_TIMEZONE,
                new PostgresTable.ColumnInfo("col", "timestamp with time zone", null, true, null, 1).getSqlType());
        assertEquals(Types.ARRAY, new PostgresTable.ColumnInfo("col", "ARRAY", null, true, null, 1).getSqlType());
    }

    @Test
    public void columnInfo_mapUserDefinedTypes() {
        // UUID type
        assertEquals(Types.OTHER,
                new PostgresTable.ColumnInfo("col", "user-defined", "uuid", true, null, 1).getSqlType());
        // JSONB type
        assertEquals(Types.OTHER,
                new PostgresTable.ColumnInfo("col", "user-defined", "jsonb", true, null, 1).getSqlType());
        // INET type
        assertEquals(Types.OTHER,
                new PostgresTable.ColumnInfo("col", "user-defined", "inet", true, null, 1).getSqlType());
        // INTERVAL type
        assertEquals(Types.OTHER,
                new PostgresTable.ColumnInfo("col", "user-defined", "interval", true, null, 1).getSqlType());
    }

    @Test
    public void columnInfo_nullDataType_returnsOther() {
        assertEquals(Types.OTHER, new PostgresTable.ColumnInfo("col", null, null, true, null, 1).getSqlType());
    }

    @Test
    public void columnInfo_getJavaType() {
        assertEquals(String.class, new PostgresTable.ColumnInfo("col", "text", null, true, null, 1).getJavaType());
        assertEquals(Integer.class, new PostgresTable.ColumnInfo("col", "integer", null, true, null, 1).getJavaType());
        assertEquals(Long.class, new PostgresTable.ColumnInfo("col", "bigint", null, true, null, 1).getJavaType());
        assertEquals(Boolean.class, new PostgresTable.ColumnInfo("col", "boolean", null, true, null, 1).getJavaType());
        assertEquals(byte[].class, new PostgresTable.ColumnInfo("col", "bytea", null, true, null, 1).getJavaType());
        assertEquals(java.time.LocalDate.class,
                new PostgresTable.ColumnInfo("col", "date", null, true, null, 1).getJavaType());
        assertEquals(java.util.UUID.class,
                new PostgresTable.ColumnInfo("col", "user-defined", "uuid", true, null, 1).getJavaType());
    }

    @Test
    public void columnInfo_toString_formatsCorrectly() {
        PostgresTable.ColumnInfo info = new PostgresTable.ColumnInfo("test_col", "text", null, false, "default_val", 1);

        String str = info.toString();
        assertTrue(str.contains("test_col"));
        assertTrue(str.contains("text"));
        assertTrue(str.contains("nullable=false"));
    }

    @Test
    public void columnInfo_getters() {
        PostgresTable.ColumnInfo info = new PostgresTable.ColumnInfo("col_name", "integer", "int4", true, "0", 5);

        assertEquals("col_name", info.getName());
        assertEquals("integer", info.getDataType());
        assertEquals("int4", info.getUdtName());
        assertTrue(info.isNullable());
        assertEquals("0", info.getDefaultValue());
        assertEquals(5, info.getOrdinalPosition());
    }
}
