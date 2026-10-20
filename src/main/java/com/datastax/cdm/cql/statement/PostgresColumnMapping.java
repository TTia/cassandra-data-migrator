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
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.datastax.cdm.schema.CqlTable;
import com.datastax.cdm.schema.PostgresTable;
import com.datastax.cdm.schema.PostgresTypeMapper;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.type.DataType;

/**
 * The PostgreSQL columns that also exist in the origin table, and their values for an origin row, converted the way
 * they are written. A column on one side only is left out, so PostgreSQL applies its default.
 */
class PostgresColumnMapping {

    private final CqlTable originTable;
    private final PostgresTypeMapper typeMapper;
    private final List<String> columns = new ArrayList<>();
    private final List<Integer> originIndexes = new ArrayList<>();

    PostgresColumnMapping(PostgresTable targetTable, CqlTable originTable, PostgresTypeMapper typeMapper) {
        this.originTable = originTable;
        this.typeMapper = typeMapper;
        List<String> originColumns = originTable.getColumnNames(false);
        for (String column : targetTable.getColumnNames()) {
            for (int i = 0; i < originColumns.size(); i++) {
                if (originColumns.get(i).equalsIgnoreCase(column)) {
                    columns.add(column);
                    originIndexes.add(i);
                    break;
                }
            }
        }
    }

    List<String> getColumns() {
        return columns;
    }

    /**
     * Returns the value of column {@code i} for the origin row, converted for PostgreSQL. A value that cannot be
     * converted fails the call rather than turning into NULL.
     */
    Object value(int i, Row originRow, Connection connection) throws SQLException {
        int originIndex = originIndexes.get(i);
        try {
            Object value = originTable.getAndConvertData(originIndex, originRow);
            if (value == null) {
                return null;
            }
            DataType originType = originTable.getColumnCqlTypes().get(originIndex);
            return typeMapper.convertValue(value, originType, connection);
        } catch (SQLException | RuntimeException e) {
            throw new SQLException("Cannot convert column " + columns.get(i) + ": " + e.getMessage(), e);
        }
    }
}
