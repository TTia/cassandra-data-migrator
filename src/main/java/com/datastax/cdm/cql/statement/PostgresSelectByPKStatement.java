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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.cdm.data.EnhancedPK;
import com.datastax.cdm.schema.CqlTable;
import com.datastax.cdm.schema.PostgresTable;

/**
 * Executes SELECT queries against PostgreSQL to fetch records by primary key. Used for validation/diff operations to
 * compare Cassandra data with PostgreSQL data.
 */
public class PostgresSelectByPKStatement {

    private static final Logger logger = LoggerFactory.getLogger(PostgresSelectByPKStatement.class);

    private final PostgresTable targetTable;
    private final CqlTable originTable;
    private final String selectStatement;
    private final List<String> primaryKeyColumns;

    /**
     * Creates a new PostgresSelectByPKStatement.
     *
     * @param targetTable
     *            the PostgreSQL target table
     * @param originTable
     *            the Cassandra origin table (for column mapping)
     */
    public PostgresSelectByPKStatement(PostgresTable targetTable, CqlTable originTable) {
        this.targetTable = targetTable;
        this.originTable = originTable;
        this.primaryKeyColumns = targetTable.getPrimaryKeyColumns();
        this.selectStatement = buildSelectStatement();

        logger.debug("PostgresSelectByPKStatement created: {}", selectStatement);
    }

    /**
     * Gets a record from PostgreSQL by primary key.
     *
     * @param connection
     *            the database connection
     * @param pk
     *            the enhanced primary key
     *
     * @return a map of column name to value, or null if not found
     *
     * @throws SQLException
     *             if query fails
     */
    public Map<String, Object> getRecord(Connection connection, EnhancedPK pk) throws SQLException {
        try (PreparedStatement stmt = connection.prepareStatement(selectStatement)) {
            bindPrimaryKey(stmt, pk);

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return extractRow(rs);
                }
                return null;
            }
        }
    }

    /**
     * Checks if a record exists in PostgreSQL by primary key.
     *
     * @param connection
     *            the database connection
     * @param pk
     *            the enhanced primary key
     *
     * @return true if the record exists
     *
     * @throws SQLException
     *             if query fails
     */
    public boolean recordExists(Connection connection, EnhancedPK pk) throws SQLException {
        String existsQuery = "SELECT 1 FROM " + targetTable.getQualifiedTableName() + " WHERE " + buildWhereClause()
                + " LIMIT 1";

        try (PreparedStatement stmt = connection.prepareStatement(existsQuery)) {
            bindPrimaryKey(stmt, pk);

            try (ResultSet rs = stmt.executeQuery()) {
                return rs.next();
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

    private String buildSelectStatement() {
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT ");

        // Column list
        List<String> columns = targetTable.getColumnNames();
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                sql.append(", ");
            }
            sql.append(quoteIdentifier(columns.get(i)));
        }

        sql.append(" FROM ").append(targetTable.getQualifiedTableName());
        sql.append(" WHERE ").append(buildWhereClause());

        return sql.toString();
    }

    private String buildWhereClause() {
        StringBuilder where = new StringBuilder();
        for (int i = 0; i < primaryKeyColumns.size(); i++) {
            if (i > 0) {
                where.append(" AND ");
            }
            where.append(quoteIdentifier(primaryKeyColumns.get(i))).append(" = ?");
        }
        return where.toString();
    }

    private void bindPrimaryKey(PreparedStatement stmt, EnhancedPK pk) throws SQLException {
        List<Object> pkValues = pk.getPKValues();

        // Map Cassandra PK values to PostgreSQL PK columns
        List<String> originPKNames = getOriginPKNames();

        for (int i = 0; i < primaryKeyColumns.size(); i++) {
            String targetPKCol = primaryKeyColumns.get(i);
            Object value = null;

            // Find corresponding origin PK value
            for (int j = 0; j < originPKNames.size(); j++) {
                if (originPKNames.get(j).equalsIgnoreCase(targetPKCol)) {
                    if (j < pkValues.size()) {
                        value = pkValues.get(j);
                    }
                    break;
                }
            }

            if (value != null) {
                stmt.setObject(i + 1, value);
            } else {
                stmt.setNull(i + 1, java.sql.Types.NULL);
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

    private Map<String, Object> extractRow(ResultSet rs) throws SQLException {
        Map<String, Object> row = new HashMap<>();
        List<String> columns = targetTable.getColumnNames();

        for (int i = 0; i < columns.size(); i++) {
            String colName = columns.get(i);
            Object value = rs.getObject(i + 1);
            row.put(colName, value);
        }

        return row;
    }

    private String quoteIdentifier(String identifier) {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }
}
