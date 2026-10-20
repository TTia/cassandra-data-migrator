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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.cdm.data.EnhancedPK;
import com.datastax.cdm.data.Record;
import com.datastax.cdm.schema.CqlTable;
import com.datastax.cdm.schema.PostgresTable;
import com.datastax.cdm.schema.PostgresTypeMapper;
import com.datastax.oss.driver.api.core.type.DataType;

/**
 * Looks up the PostgreSQL row for an origin record and compares it with the origin values. The compare runs in
 * PostgreSQL: each origin value is bound exactly as the upsert binds it and tested with {@code IS NOT DISTINCT FROM},
 * so a column compares equal exactly when writing the origin value again would not change it.
 */
public class PostgresSelectByPKStatement {

    private static final Logger logger = LoggerFactory.getLogger(PostgresSelectByPKStatement.class);

    private final CqlTable originTable;
    private final PostgresColumnMapping mapping;
    private final List<String> primaryKeyColumns;
    // Indexes into the mapping of the non-PK columns that are compared
    private final List<Integer> compared = new ArrayList<>();
    private final String selectStatement;
    private final PostgresTypeMapper typeMapper = new PostgresTypeMapper();

    /**
     * Creates a new PostgresSelectByPKStatement.
     *
     * @param targetTable
     *            the PostgreSQL target table
     * @param originTable
     *            the Cassandra origin table (for column mapping)
     */
    public PostgresSelectByPKStatement(PostgresTable targetTable, CqlTable originTable) {
        this.originTable = originTable;
        this.mapping = new PostgresColumnMapping(targetTable, originTable, typeMapper);
        this.primaryKeyColumns = targetTable.getPrimaryKeyColumns();
        if (primaryKeyColumns.isEmpty()) {
            throw new IllegalStateException("Validation looks rows up by primary key, and "
                    + targetTable.getQualifiedTableName() + " has none");
        }
        // A key column the origin cannot fill would be looked up as NULL: every row MISSING, and autocorrect
        // inserting a duplicate of each
        List<String> originPKNames = getOriginPKNames();
        for (String pk : primaryKeyColumns) {
            if (originPKNames.stream().noneMatch(pk::equalsIgnoreCase)) {
                throw new IllegalStateException(
                        "Validation looks rows up by the primary key of " + targetTable.getQualifiedTableName()
                                + ", and its column " + pk + " is not a key column of the origin table");
            }
        }
        List<String> columns = mapping.getColumns();
        for (int i = 0; i < columns.size(); i++) {
            if (!primaryKeyColumns.contains(columns.get(i))) {
                compared.add(i);
            }
        }
        this.selectStatement = buildSelectStatement(targetTable);

        logger.debug("PostgresSelectByPKStatement created: {}", selectStatement);
    }

    /**
     * Compares an origin record with its PostgreSQL row.
     *
     * @param connection
     *            the database connection
     * @param record
     *            the origin record
     *
     * @return null when the row is missing, otherwise one {@code column origin[..] target[..]} entry per column that
     *         differs (empty when the row matches)
     *
     * @throws SQLException
     *             if an origin value cannot be converted or the query fails
     */
    public List<String> findMismatches(Connection connection, Record record) throws SQLException {
        List<Object> originValues = new ArrayList<>();
        try (PreparedStatement stmt = connection.prepareStatement(selectStatement)) {
            for (int i = 0; i < compared.size(); i++) {
                Object value = mapping.value(compared.get(i), record.getOriginRow(), connection);
                originValues.add(value);
                if (value == null) {
                    stmt.setNull(i + 1, java.sql.Types.NULL);
                } else {
                    stmt.setObject(i + 1, value);
                }
            }
            bindPrimaryKey(stmt, record.getPk(), connection, compared.size());

            try (ResultSet rs = stmt.executeQuery()) {
                if (!rs.next()) {
                    return null;
                }
                List<String> mismatches = new ArrayList<>();
                for (int i = 0; i < compared.size(); i++) {
                    if (!rs.getBoolean(2 * i + 2)) {
                        mismatches.add(mapping.getColumns().get(compared.get(i)) + " origin[" + originValues.get(i)
                                + "] target[" + rs.getString(2 * i + 1) + "]");
                    }
                }
                return mismatches;
            }
        }
    }

    /**
     * Gets the SELECT SQL statement.
     *
     * @return the SELECT statement
     */
    public String getSelectStatement() {
        return selectStatement;
    }

    /**
     * Gets the primary key columns.
     *
     * @return the list of primary key column names
     */
    public List<String> getPrimaryKeyColumns() {
        return primaryKeyColumns;
    }

    // ==================== Private Methods ====================

    // shortcut: the parameter keeps the type the upsert binds, so a column whose type has no equality operator with
    // it (json, or a CQL type that only converts to the column type on assignment) fails the partition with an SQL
    // error; cast the parameter to the column type if a real schema needs it
    private String buildSelectStatement(PostgresTable targetTable) {
        List<String> selected = new ArrayList<>();
        for (int i : compared) {
            String column = PostgresTable.quoteIdentifier(mapping.getColumns().get(i));
            selected.add(column);
            selected.add("(" + column + " IS NOT DISTINCT FROM ?)");
        }
        List<String> where = new ArrayList<>();
        for (String pk : primaryKeyColumns) {
            where.add(PostgresTable.quoteIdentifier(pk) + " = ?");
        }
        return "SELECT " + (selected.isEmpty() ? "1" : String.join(", ", selected)) + " FROM "
                + targetTable.getQualifiedTableName() + " WHERE " + String.join(" AND ", where);
    }

    private void bindPrimaryKey(PreparedStatement stmt, EnhancedPK pk, Connection connection, int offset)
            throws SQLException {
        List<Object> pkValues = pk.getPKValues();

        // Map Cassandra PK values to PostgreSQL PK columns
        List<String> originPKNames = getOriginPKNames();

        for (int i = 0; i < primaryKeyColumns.size(); i++) {
            String targetPKCol = primaryKeyColumns.get(i);
            Object value = null;
            DataType originType = null;

            // Find corresponding origin PK value
            for (int j = 0; j < originPKNames.size(); j++) {
                if (originPKNames.get(j).equalsIgnoreCase(targetPKCol)) {
                    if (j < pkValues.size()) {
                        value = pkValues.get(j);
                        originType = originTable.getDataType(originPKNames.get(j));
                    }
                    break;
                }
            }

            if (value != null) {
                // Same conversion as the upsert, so a timestamp key binds as OffsetDateTime, not Instant
                stmt.setObject(offset + i + 1, typeMapper.convertValue(value, originType, connection));
            } else {
                stmt.setNull(offset + i + 1, java.sql.Types.NULL);
            }
        }
    }

    private List<String> getOriginPKNames() {
        // Get primary key column names from origin table
        List<String> pkNames = new ArrayList<>();
        pkNames.addAll(originTable.getPartitionKeyNames(false));
        // Add clustering keys if any
        try {
            // Attempt to get full PK which includes clustering columns
            List<String> allPK = originTable.getPKNames(false);
            if (allPK != null && allPK.size() > pkNames.size()) {
                for (String col : allPK) {
                    if (!pkNames.contains(col)) {
                        pkNames.add(col);
                    }
                }
            }
        } catch (Exception e) {
            // If getPKNames fails, just use partition keys
            logger.debug("Could not get full PK names, using partition keys only");
        }
        return pkNames;
    }
}
