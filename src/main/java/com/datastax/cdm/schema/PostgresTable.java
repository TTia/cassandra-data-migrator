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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.cdm.properties.IPropertyHelper;
import com.datastax.cdm.properties.KnownProperties;

/**
 * Represents a PostgreSQL table and provides schema metadata discovery.
 * Discovers column information from the PostgreSQL information_schema.
 */
public class PostgresTable {

    private static final Logger logger = LoggerFactory.getLogger(PostgresTable.class);

    private final String schemaName;
    private final String tableName;
    private final List<ColumnInfo> columns = new ArrayList<>();
    private final List<String> primaryKeyColumns = new ArrayList<>();
    private boolean metadataLoaded = false;

    /**
     * Schema discovery query that retrieves column information and primary key constraints.
     */
    private static final String COLUMN_QUERY =
            "SELECT " +
            "    c.column_name, " +
            "    c.data_type, " +
            "    c.udt_name, " +
            "    c.is_nullable, " +
            "    c.column_default, " +
            "    c.ordinal_position " +
            "FROM information_schema.columns c " +
            "WHERE c.table_schema = ? AND c.table_name = ? " +
            "ORDER BY c.ordinal_position";

    private static final String PRIMARY_KEY_QUERY =
            "SELECT kcu.column_name " +
            "FROM information_schema.table_constraints tc " +
            "JOIN information_schema.key_column_usage kcu " +
            "    ON tc.constraint_name = kcu.constraint_name " +
            "    AND tc.table_schema = kcu.table_schema " +
            "WHERE tc.constraint_type = 'PRIMARY KEY' " +
            "    AND tc.table_schema = ? " +
            "    AND tc.table_name = ? " +
            "ORDER BY kcu.ordinal_position";

    /**
     * Creates a PostgresTable from property helper configuration.
     *
     * @param propertyHelper the property helper to read configuration from
     */
    public PostgresTable(IPropertyHelper propertyHelper) {
        String schema = propertyHelper.getString(KnownProperties.PG_SCHEMA);
        String table = propertyHelper.getString(KnownProperties.PG_TABLE);

        // Fall back to origin keyspace.table parsing if PG table not specified
        if (table == null || table.isEmpty()) {
            String originKeyspaceTable = propertyHelper.getString(KnownProperties.ORIGIN_KEYSPACE_TABLE);
            if (originKeyspaceTable != null && originKeyspaceTable.contains(".")) {
                String[] parts = originKeyspaceTable.split("\\.");
                this.schemaName = schema != null && !schema.isEmpty() ? schema : parts[0];
                this.tableName = parts[1];
            } else if (originKeyspaceTable != null) {
                this.schemaName = schema != null && !schema.isEmpty() ? schema : "public";
                this.tableName = originKeyspaceTable;
            } else {
                throw new IllegalArgumentException("PostgreSQL table name is required");
            }
        } else {
            this.schemaName = schema != null && !schema.isEmpty() ? schema : "public";
            this.tableName = table;
        }
    }

    /**
     * Creates a PostgresTable with explicit schema and table names.
     *
     * @param schemaName the PostgreSQL schema name
     * @param tableName  the PostgreSQL table name
     */
    public PostgresTable(String schemaName, String tableName) {
        this.schemaName = schemaName != null ? schemaName : "public";
        this.tableName = tableName;
    }

    /**
     * Loads table metadata from the database using the provided connection.
     *
     * @param connection the database connection
     * @throws SQLException if metadata cannot be loaded
     */
    public void loadMetadata(Connection connection) throws SQLException {
        if (metadataLoaded) {
            return;
        }

        logger.info("Loading PostgreSQL table metadata for {}.{}", schemaName, tableName);

        // Load column information
        try (PreparedStatement stmt = connection.prepareStatement(COLUMN_QUERY)) {
            stmt.setString(1, schemaName);
            stmt.setString(2, tableName);

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    ColumnInfo columnInfo = new ColumnInfo(
                            rs.getString("column_name"),
                            rs.getString("data_type"),
                            rs.getString("udt_name"),
                            "YES".equalsIgnoreCase(rs.getString("is_nullable")),
                            rs.getString("column_default"),
                            rs.getInt("ordinal_position"));
                    columns.add(columnInfo);
                }
            }
        }

        if (columns.isEmpty()) {
            throw new SQLException(
                    "Table " + schemaName + "." + tableName + " not found or has no columns");
        }

        // Load primary key columns
        try (PreparedStatement stmt = connection.prepareStatement(PRIMARY_KEY_QUERY)) {
            stmt.setString(1, schemaName);
            stmt.setString(2, tableName);

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    primaryKeyColumns.add(rs.getString("column_name"));
                }
            }
        }

        metadataLoaded = true;
        logger.info("Loaded metadata for {}.{}: {} columns, {} primary key columns",
                schemaName, tableName, columns.size(), primaryKeyColumns.size());
    }

    /**
     * Returns the schema name.
     *
     * @return the schema name
     */
    public String getSchemaName() {
        return schemaName;
    }

    /**
     * Returns the table name.
     *
     * @return the table name
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * Returns the fully qualified table name (schema.table).
     *
     * @return the qualified table name
     */
    public String getQualifiedTableName() {
        return schemaName + "." + tableName;
    }

    /**
     * Returns the list of column names.
     *
     * @return the column names
     */
    public List<String> getColumnNames() {
        List<String> names = new ArrayList<>();
        for (ColumnInfo col : columns) {
            names.add(col.getName());
        }
        return names;
    }

    /**
     * Returns the list of column information.
     *
     * @return the column information list
     */
    public List<ColumnInfo> getColumns() {
        return Collections.unmodifiableList(columns);
    }

    /**
     * Returns the primary key column names.
     *
     * @return the primary key column names
     */
    public List<String> getPrimaryKeyColumns() {
        return Collections.unmodifiableList(primaryKeyColumns);
    }

    /**
     * Checks if the table has ON CONFLICT support (PostgreSQL 9.5+).
     *
     * @param connection the database connection
     * @return true if ON CONFLICT is supported
     */
    public boolean hasOnConflictSupport(Connection connection) {
        try {
            int majorVersion = connection.getMetaData().getDatabaseMajorVersion();
            int minorVersion = connection.getMetaData().getDatabaseMinorVersion();
            return majorVersion > 9 || (majorVersion == 9 && minorVersion >= 5);
        } catch (SQLException e) {
            logger.warn("Could not determine PostgreSQL version, assuming ON CONFLICT support", e);
            return true;
        }
    }

    /**
     * Returns the SQL type constant for a column.
     *
     * @param columnIndex the column index (0-based)
     * @return the java.sql.Types constant
     */
    public int getSqlType(int columnIndex) {
        if (columnIndex < 0 || columnIndex >= columns.size()) {
            throw new IndexOutOfBoundsException("Invalid column index: " + columnIndex);
        }
        return columns.get(columnIndex).getSqlType();
    }

    /**
     * Returns the SQL type constant for a column by name.
     *
     * @param columnName the column name
     * @return the java.sql.Types constant
     */
    public int getSqlType(String columnName) {
        for (ColumnInfo col : columns) {
            if (col.getName().equalsIgnoreCase(columnName)) {
                return col.getSqlType();
            }
        }
        throw new IllegalArgumentException("Column not found: " + columnName);
    }

    /**
     * Returns the column index for a given column name.
     *
     * @param columnName the column name
     * @return the column index (0-based)
     */
    public int getColumnIndex(String columnName) {
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).getName().equalsIgnoreCase(columnName)) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Checks if metadata has been loaded.
     *
     * @return true if metadata is loaded
     */
    public boolean isMetadataLoaded() {
        return metadataLoaded;
    }

    /**
     * Represents information about a PostgreSQL column.
     */
    public static class ColumnInfo {
        private final String name;
        private final String dataType;
        private final String udtName;
        private final boolean nullable;
        private final String defaultValue;
        private final int ordinalPosition;

        public ColumnInfo(String name, String dataType, String udtName,
                boolean nullable, String defaultValue, int ordinalPosition) {
            this.name = name;
            this.dataType = dataType;
            this.udtName = udtName;
            this.nullable = nullable;
            this.defaultValue = defaultValue;
            this.ordinalPosition = ordinalPosition;
        }

        public String getName() {
            return name;
        }

        public String getDataType() {
            return dataType;
        }

        public String getUdtName() {
            return udtName;
        }

        public boolean isNullable() {
            return nullable;
        }

        public String getDefaultValue() {
            return defaultValue;
        }

        public int getOrdinalPosition() {
            return ordinalPosition;
        }

        /**
         * Returns the java.sql.Types constant for this column's data type.
         *
         * @return the SQL type constant
         */
        public int getSqlType() {
            return mapDataTypeToSqlType(dataType, udtName);
        }

        /**
         * Returns the Java class for binding values to this column.
         *
         * @return the Java class
         */
        public Class<?> getJavaType() {
            return mapSqlTypeToJavaClass(getSqlType(), udtName);
        }

        private static int mapDataTypeToSqlType(String dataType, String udtName) {
            if (dataType == null) {
                return Types.OTHER;
            }

            String lowerDataType = dataType.toLowerCase();

            // Handle ARRAY types
            if (lowerDataType.equals("array")) {
                return Types.ARRAY;
            }

            // Handle USER-DEFINED types (includes JSONB, UUID, etc.)
            if (lowerDataType.equals("user-defined")) {
                if (udtName != null) {
                    String lowerUdt = udtName.toLowerCase();
                    if (lowerUdt.equals("jsonb") || lowerUdt.equals("json")) {
                        return Types.OTHER;
                    }
                    if (lowerUdt.equals("uuid")) {
                        return Types.OTHER;
                    }
                    if (lowerUdt.equals("inet")) {
                        return Types.OTHER;
                    }
                    if (lowerUdt.equals("interval")) {
                        return Types.OTHER;
                    }
                }
                return Types.OTHER;
            }

            switch (lowerDataType) {
                case "text":
                case "character varying":
                case "varchar":
                case "character":
                case "char":
                    return Types.VARCHAR;
                case "integer":
                case "int":
                case "int4":
                    return Types.INTEGER;
                case "bigint":
                case "int8":
                    return Types.BIGINT;
                case "smallint":
                case "int2":
                    return Types.SMALLINT;
                case "real":
                case "float4":
                    return Types.REAL;
                case "double precision":
                case "float8":
                    return Types.DOUBLE;
                case "numeric":
                case "decimal":
                    return Types.NUMERIC;
                case "boolean":
                case "bool":
                    return Types.BOOLEAN;
                case "bytea":
                    return Types.BINARY;
                case "date":
                    return Types.DATE;
                case "time":
                case "time without time zone":
                    return Types.TIME;
                case "time with time zone":
                case "timetz":
                    return Types.TIME_WITH_TIMEZONE;
                case "timestamp":
                case "timestamp without time zone":
                    return Types.TIMESTAMP;
                case "timestamp with time zone":
                case "timestamptz":
                    return Types.TIMESTAMP_WITH_TIMEZONE;
                case "uuid":
                case "json":
                case "jsonb":
                case "inet":
                case "interval":
                    return Types.OTHER;
                default:
                    return Types.OTHER;
            }
        }

        private static Class<?> mapSqlTypeToJavaClass(int sqlType, String udtName) {
            switch (sqlType) {
                case Types.VARCHAR:
                case Types.CHAR:
                case Types.LONGVARCHAR:
                    return String.class;
                case Types.INTEGER:
                    return Integer.class;
                case Types.BIGINT:
                    return Long.class;
                case Types.SMALLINT:
                    return Short.class;
                case Types.REAL:
                    return Float.class;
                case Types.DOUBLE:
                    return Double.class;
                case Types.NUMERIC:
                case Types.DECIMAL:
                    return java.math.BigDecimal.class;
                case Types.BOOLEAN:
                    return Boolean.class;
                case Types.BINARY:
                case Types.VARBINARY:
                case Types.LONGVARBINARY:
                    return byte[].class;
                case Types.DATE:
                    return java.time.LocalDate.class;
                case Types.TIME:
                    return java.time.LocalTime.class;
                case Types.TIMESTAMP:
                    return java.time.LocalDateTime.class;
                case Types.TIMESTAMP_WITH_TIMEZONE:
                    return java.time.OffsetDateTime.class;
                case Types.ARRAY:
                    return java.sql.Array.class;
                case Types.OTHER:
                    if (udtName != null) {
                        String lowerUdt = udtName.toLowerCase();
                        if (lowerUdt.equals("uuid")) {
                            return java.util.UUID.class;
                        }
                    }
                    return Object.class;
                default:
                    return Object.class;
            }
        }

        @Override
        public String toString() {
            return String.format("ColumnInfo{name='%s', dataType='%s', udtName='%s', nullable=%b}",
                    name, dataType, udtName, nullable);
        }
    }
}
