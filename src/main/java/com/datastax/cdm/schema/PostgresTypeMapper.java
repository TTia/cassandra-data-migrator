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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.postgresql.util.PGobject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.oss.driver.api.core.data.CqlDuration;
import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.ListType;
import com.datastax.oss.driver.api.core.type.MapType;
import com.datastax.oss.driver.api.core.type.SetType;
import com.datastax.oss.driver.api.core.type.TupleType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.VectorType;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * Maps Cassandra types to PostgreSQL types and handles value conversion. Provides complete type mapping for all
 * Cassandra data types.
 */
public class PostgresTypeMapper {

    private static final Logger logger = LoggerFactory.getLogger(PostgresTypeMapper.class);
    private static final Gson gson = new GsonBuilder().serializeNulls().create();

    /**
     * Information about a PostgreSQL type mapping.
     */
    public static class PostgresType {
        private final String typeName;
        private final int sqlType;
        private final Class<?> javaClass;
        private final String arrayElementType;

        public PostgresType(String typeName, int sqlType, Class<?> javaClass) {
            this(typeName, sqlType, javaClass, null);
        }

        public PostgresType(String typeName, int sqlType, Class<?> javaClass, String arrayElementType) {
            this.typeName = typeName;
            this.sqlType = sqlType;
            this.javaClass = javaClass;
            this.arrayElementType = arrayElementType;
        }

        public String getTypeName() {
            return typeName;
        }

        public int getSqlType() {
            return sqlType;
        }

        public Class<?> getJavaClass() {
            return javaClass;
        }

        public String getArrayElementType() {
            return arrayElementType;
        }

        public boolean isArray() {
            return sqlType == Types.ARRAY;
        }

        public boolean isJsonb() {
            return "jsonb".equalsIgnoreCase(typeName);
        }

        @Override
        public String toString() {
            return String.format("PostgresType{typeName='%s', sqlType=%d}", typeName, sqlType);
        }
    }

    /**
     * Maps a Cassandra DataType to a PostgreSQL type.
     *
     * @param cassandraType
     *            the Cassandra data type
     *
     * @return the corresponding PostgreSQL type
     */
    public PostgresType mapCassandraType(DataType cassandraType) {
        if (cassandraType == null) {
            return new PostgresType("text", Types.VARCHAR, String.class);
        }

        // Handle primitive types
        if (cassandraType.equals(DataTypes.TEXT) || cassandraType.equals(DataTypes.ASCII)) {
            return new PostgresType("text", Types.VARCHAR, String.class);
        }
        if (cassandraType.equals(DataTypes.INT)) {
            return new PostgresType("integer", Types.INTEGER, Integer.class);
        }
        if (cassandraType.equals(DataTypes.BIGINT)) {
            return new PostgresType("bigint", Types.BIGINT, Long.class);
        }
        if (cassandraType.equals(DataTypes.SMALLINT)) {
            return new PostgresType("smallint", Types.SMALLINT, Short.class);
        }
        if (cassandraType.equals(DataTypes.TINYINT)) {
            return new PostgresType("smallint", Types.SMALLINT, Short.class);
        }
        if (cassandraType.equals(DataTypes.FLOAT)) {
            return new PostgresType("real", Types.REAL, Float.class);
        }
        if (cassandraType.equals(DataTypes.DOUBLE)) {
            return new PostgresType("double precision", Types.DOUBLE, Double.class);
        }
        if (cassandraType.equals(DataTypes.DECIMAL)) {
            return new PostgresType("numeric", Types.NUMERIC, BigDecimal.class);
        }
        if (cassandraType.equals(DataTypes.VARINT)) {
            return new PostgresType("numeric", Types.NUMERIC, BigDecimal.class);
        }
        if (cassandraType.equals(DataTypes.BOOLEAN)) {
            return new PostgresType("boolean", Types.BOOLEAN, Boolean.class);
        }
        if (cassandraType.equals(DataTypes.UUID) || cassandraType.equals(DataTypes.TIMEUUID)) {
            return new PostgresType("uuid", Types.OTHER, UUID.class);
        }
        if (cassandraType.equals(DataTypes.TIMESTAMP)) {
            return new PostgresType("timestamp with time zone", Types.TIMESTAMP_WITH_TIMEZONE, OffsetDateTime.class);
        }
        if (cassandraType.equals(DataTypes.DATE)) {
            return new PostgresType("date", Types.DATE, LocalDate.class);
        }
        if (cassandraType.equals(DataTypes.TIME)) {
            return new PostgresType("time", Types.TIME, LocalTime.class);
        }
        if (cassandraType.equals(DataTypes.DURATION)) {
            return new PostgresType("interval", Types.OTHER, PGobject.class);
        }
        if (cassandraType.equals(DataTypes.BLOB)) {
            return new PostgresType("bytea", Types.BINARY, byte[].class);
        }
        if (cassandraType.equals(DataTypes.INET)) {
            return new PostgresType("inet", Types.OTHER, PGobject.class);
        }

        // Handle collection types
        if (cassandraType instanceof ListType) {
            ListType listType = (ListType) cassandraType;
            String elementPgType = getArrayElementTypeName(listType.getElementType());
            return new PostgresType(elementPgType + "[]", Types.ARRAY, java.sql.Array.class, elementPgType);
        }
        if (cassandraType instanceof SetType) {
            SetType setType = (SetType) cassandraType;
            String elementPgType = getArrayElementTypeName(setType.getElementType());
            return new PostgresType(elementPgType + "[]", Types.ARRAY, java.sql.Array.class, elementPgType);
        }

        // Handle MAP, UDT, Tuple as JSONB
        if (cassandraType instanceof MapType) {
            return new PostgresType("jsonb", Types.OTHER, PGobject.class);
        }
        if (cassandraType instanceof UserDefinedType) {
            return new PostgresType("jsonb", Types.OTHER, PGobject.class);
        }
        if (cassandraType instanceof TupleType) {
            return new PostgresType("jsonb", Types.OTHER, PGobject.class);
        }

        // Handle Vector type (requires pgvector extension)
        if (cassandraType instanceof VectorType) {
            return new PostgresType("vector", Types.OTHER, PGobject.class);
        }

        // Default to text for unknown types
        logger.warn("Unknown Cassandra type: {}, defaulting to text", cassandraType);
        return new PostgresType("text", Types.VARCHAR, String.class);
    }

    /**
     * Converts a Cassandra value to a PostgreSQL-compatible value.
     *
     * @param cassandraValue
     *            the value from Cassandra
     * @param fromType
     *            the Cassandra data type
     * @param connection
     *            the PostgreSQL connection (for creating arrays)
     *
     * @return the converted value suitable for PostgreSQL
     *
     * @throws SQLException
     *             if conversion fails
     */
    public Object convertValue(Object cassandraValue, DataType fromType, Connection connection) throws SQLException {
        if (cassandraValue == null) {
            return null;
        }

        // Handle primitive types that need conversion
        if (cassandraValue instanceof ByteBuffer) {
            return convertBlob((ByteBuffer) cassandraValue);
        }
        if (cassandraValue instanceof Instant) {
            return convertTimestamp((Instant) cassandraValue);
        }
        if (cassandraValue instanceof CqlDuration) {
            return convertDuration((CqlDuration) cassandraValue);
        }
        if (cassandraValue instanceof InetAddress) {
            return convertInet((InetAddress) cassandraValue);
        }
        if (cassandraValue instanceof BigInteger) {
            return new BigDecimal((BigInteger) cassandraValue);
        }

        // Handle collection types
        if (cassandraValue instanceof List && fromType instanceof ListType) {
            ListType listType = (ListType) fromType;
            return convertList((List<?>) cassandraValue, connection, listType.getElementType());
        }
        if (cassandraValue instanceof Set && fromType instanceof SetType) {
            SetType setType = (SetType) fromType;
            return convertSet((Set<?>) cassandraValue, connection, setType.getElementType());
        }
        if (cassandraValue instanceof Map) {
            return convertMap((Map<?, ?>) cassandraValue);
        }

        // Handle UDT and Tuple
        if (cassandraValue instanceof UdtValue) {
            return convertUDT((UdtValue) cassandraValue);
        }
        if (cassandraValue instanceof TupleValue) {
            return convertTuple((TupleValue) cassandraValue);
        }

        // Handle Vector type
        if (fromType instanceof VectorType && cassandraValue instanceof List) {
            return convertVector((List<?>) cassandraValue);
        }

        // Return as-is for primitive types that don't need conversion
        return cassandraValue;
    }

    /**
     * Converts a Cassandra LIST to a PostgreSQL ARRAY.
     *
     * @param list
     *            the list to convert
     * @param connection
     *            the PostgreSQL connection
     * @param elementType
     *            the Cassandra element type
     *
     * @return a PostgreSQL Array
     *
     * @throws SQLException
     *             if conversion fails
     */
    public java.sql.Array convertList(List<?> list, Connection connection, DataType elementType) throws SQLException {
        String pgElementType = getArrayElementTypeName(elementType);
        Object[] elements = convertCollectionElements(list, elementType);
        return connection.createArrayOf(pgElementType, elements);
    }

    /**
     * Converts a Cassandra SET to a PostgreSQL ARRAY.
     *
     * @param set
     *            the set to convert
     * @param connection
     *            the PostgreSQL connection
     * @param elementType
     *            the Cassandra element type
     *
     * @return a PostgreSQL Array
     *
     * @throws SQLException
     *             if conversion fails
     */
    public java.sql.Array convertSet(Set<?> set, Connection connection, DataType elementType) throws SQLException {
        String pgElementType = getArrayElementTypeName(elementType);
        Object[] elements = convertCollectionElements(new ArrayList<>(set), elementType);
        return connection.createArrayOf(pgElementType, elements);
    }

    /**
     * Converts a Cassandra MAP to a PostgreSQL JSONB.
     *
     * @param map
     *            the map to convert
     *
     * @return a PGobject representing JSONB
     *
     * @throws SQLException
     *             if conversion fails
     */
    public PGobject convertMap(Map<?, ?> map) throws SQLException {
        PGobject jsonb = new PGobject();
        jsonb.setType("jsonb");

        Map<String, Object> convertedMap = new HashMap<>();
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            String key = entry.getKey() != null ? entry.getKey().toString() : "null";
            Object value = convertValueForJson(entry.getValue());
            convertedMap.put(key, value);
        }
        jsonb.setValue(gson.toJson(convertedMap));
        return jsonb;
    }

    /**
     * Converts a Cassandra UDT to a PostgreSQL JSONB.
     *
     * @param udt
     *            the UDT value to convert
     *
     * @return a PGobject representing JSONB
     *
     * @throws SQLException
     *             if conversion fails
     */
    public PGobject convertUDT(UdtValue udt) throws SQLException {
        PGobject jsonb = new PGobject();
        jsonb.setType("jsonb");

        Map<String, Object> fields = new HashMap<>();
        UserDefinedType udtType = udt.getType();
        List<String> fieldNames = udtType.getFieldNames().stream().map(id -> id.asInternal())
                .collect(Collectors.toList());

        for (int i = 0; i < fieldNames.size(); i++) {
            String fieldName = fieldNames.get(i);
            Object value = udt.getObject(i);
            fields.put(fieldName, convertValueForJson(value));
        }
        jsonb.setValue(gson.toJson(fields));
        return jsonb;
    }

    /**
     * Converts a Cassandra Tuple to a PostgreSQL JSONB array.
     *
     * @param tuple
     *            the tuple value to convert
     *
     * @return a PGobject representing JSONB
     *
     * @throws SQLException
     *             if conversion fails
     */
    public PGobject convertTuple(TupleValue tuple) throws SQLException {
        PGobject jsonb = new PGobject();
        jsonb.setType("jsonb");

        List<Object> elements = new ArrayList<>();
        TupleType tupleType = tuple.getType();
        List<DataType> componentTypes = tupleType.getComponentTypes();

        for (int i = 0; i < componentTypes.size(); i++) {
            Object value = tuple.getObject(i);
            elements.add(convertValueForJson(value));
        }
        jsonb.setValue(gson.toJson(elements));
        return jsonb;
    }

    /**
     * Converts a Cassandra BLOB to a PostgreSQL BYTEA.
     *
     * @param blob
     *            the ByteBuffer to convert
     *
     * @return a byte array
     */
    public byte[] convertBlob(ByteBuffer blob) {
        if (blob == null) {
            return null;
        }
        byte[] bytes = new byte[blob.remaining()];
        blob.duplicate().get(bytes);
        return bytes;
    }

    /**
     * Converts a Cassandra TIMESTAMP to PostgreSQL TIMESTAMP WITH TIME ZONE.
     *
     * @param instant
     *            the instant to convert
     *
     * @return an OffsetDateTime
     */
    public OffsetDateTime convertTimestamp(Instant instant) {
        if (instant == null) {
            return null;
        }
        return instant.atOffset(ZoneOffset.UTC);
    }

    /**
     * Converts a Cassandra DURATION to a PostgreSQL INTERVAL.
     *
     * @param duration
     *            the CqlDuration to convert
     *
     * @return a PGobject representing an interval
     *
     * @throws SQLException
     *             if conversion fails
     */
    public PGobject convertDuration(CqlDuration duration) throws SQLException {
        if (duration == null) {
            return null;
        }
        PGobject interval = new PGobject();
        interval.setType("interval");

        // Format: "X months Y days Z nanoseconds"
        long totalNanos = duration.getNanoseconds();
        long seconds = totalNanos / 1_000_000_000L;
        long remainingNanos = totalNanos % 1_000_000_000L;
        long hours = seconds / 3600;
        long minutes = (seconds % 3600) / 60;
        long secs = seconds % 60;

        StringBuilder sb = new StringBuilder();
        if (duration.getMonths() != 0) {
            sb.append(duration.getMonths()).append(" months ");
        }
        if (duration.getDays() != 0) {
            sb.append(duration.getDays()).append(" days ");
        }
        if (hours != 0 || minutes != 0 || secs != 0 || remainingNanos != 0) {
            sb.append(String.format("%02d:%02d:%02d", hours, minutes, secs));
            if (remainingNanos != 0) {
                sb.append(String.format(".%09d", remainingNanos));
            }
        }

        String intervalStr = sb.toString().trim();
        if (intervalStr.isEmpty()) {
            intervalStr = "0";
        }
        interval.setValue(intervalStr);
        return interval;
    }

    /**
     * Converts a Cassandra INET to a PostgreSQL INET.
     *
     * @param inet
     *            the InetAddress to convert
     *
     * @return a PGobject representing an inet
     *
     * @throws SQLException
     *             if conversion fails
     */
    public PGobject convertInet(InetAddress inet) throws SQLException {
        if (inet == null) {
            return null;
        }
        PGobject pgInet = new PGobject();
        pgInet.setType("inet");
        pgInet.setValue(inet.getHostAddress());
        return pgInet;
    }

    /**
     * Converts a Cassandra VECTOR to a PostgreSQL vector (pgvector).
     *
     * @param vector
     *            the vector elements
     *
     * @return a PGobject representing a vector
     *
     * @throws SQLException
     *             if conversion fails
     */
    public PGobject convertVector(List<?> vector) throws SQLException {
        if (vector == null) {
            return null;
        }
        PGobject pgVector = new PGobject();
        pgVector.setType("vector");

        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < vector.size(); i++) {
            if (i > 0) {
                sb.append(",");
            }
            sb.append(vector.get(i));
        }
        sb.append("]");
        pgVector.setValue(sb.toString());
        return pgVector;
    }

    /**
     * Gets the PostgreSQL array element type name for a Cassandra type.
     */
    private String getArrayElementTypeName(DataType elementType) {
        if (elementType.equals(DataTypes.TEXT) || elementType.equals(DataTypes.ASCII)) {
            return "text";
        }
        if (elementType.equals(DataTypes.INT)) {
            return "integer";
        }
        if (elementType.equals(DataTypes.BIGINT)) {
            return "bigint";
        }
        if (elementType.equals(DataTypes.SMALLINT) || elementType.equals(DataTypes.TINYINT)) {
            return "smallint";
        }
        if (elementType.equals(DataTypes.FLOAT)) {
            return "real";
        }
        if (elementType.equals(DataTypes.DOUBLE)) {
            return "double precision";
        }
        if (elementType.equals(DataTypes.DECIMAL) || elementType.equals(DataTypes.VARINT)) {
            return "numeric";
        }
        if (elementType.equals(DataTypes.BOOLEAN)) {
            return "boolean";
        }
        if (elementType.equals(DataTypes.UUID) || elementType.equals(DataTypes.TIMEUUID)) {
            return "uuid";
        }
        if (elementType.equals(DataTypes.TIMESTAMP)) {
            return "timestamptz";
        }
        if (elementType.equals(DataTypes.DATE)) {
            return "date";
        }
        if (elementType.equals(DataTypes.TIME)) {
            return "time";
        }
        if (elementType.equals(DataTypes.BLOB)) {
            return "bytea";
        }
        // For complex nested types, use JSONB
        if (elementType instanceof MapType || elementType instanceof UserDefinedType || elementType instanceof ListType
                || elementType instanceof SetType) {
            return "jsonb";
        }
        return "text";
    }

    /**
     * Converts collection elements to PostgreSQL-compatible values.
     */
    private Object[] convertCollectionElements(Collection<?> collection, DataType elementType) {
        List<Object> converted = new ArrayList<>();
        for (Object element : collection) {
            converted.add(convertElementForArray(element, elementType));
        }
        return converted.toArray();
    }

    /**
     * Converts a single element for PostgreSQL array insertion.
     */
    private Object convertElementForArray(Object element, DataType elementType) {
        if (element == null) {
            return null;
        }

        // Handle nested collections/maps as JSONB strings
        if (element instanceof Map || element instanceof UdtValue || element instanceof List
                || element instanceof Set) {
            return gson.toJson(convertValueForJson(element));
        }

        // Handle special types
        if (element instanceof ByteBuffer) {
            return convertBlob((ByteBuffer) element);
        }
        if (element instanceof Instant) {
            return convertTimestamp((Instant) element);
        }
        if (element instanceof BigInteger) {
            return new BigDecimal((BigInteger) element);
        }

        return element;
    }

    /**
     * Converts a value for JSON serialization.
     */
    private Object convertValueForJson(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Map) {
            Map<?, ?> map = (Map<?, ?>) value;
            Map<String, Object> converted = new HashMap<>();
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                String key = entry.getKey() != null ? entry.getKey().toString() : "null";
                converted.put(key, convertValueForJson(entry.getValue()));
            }
            return converted;
        }
        if (value instanceof List) {
            List<?> list = (List<?>) value;
            List<Object> converted = new ArrayList<>();
            for (Object element : list) {
                converted.add(convertValueForJson(element));
            }
            return converted;
        }
        if (value instanceof Set) {
            Set<?> set = (Set<?>) value;
            List<Object> converted = new ArrayList<>();
            for (Object element : set) {
                converted.add(convertValueForJson(element));
            }
            return converted;
        }
        if (value instanceof UdtValue) {
            UdtValue udtValue = (UdtValue) value;
            Map<String, Object> fields = new HashMap<>();
            UserDefinedType udtType = udtValue.getType();
            List<String> fieldNames = udtType.getFieldNames().stream().map(id -> id.asInternal())
                    .collect(Collectors.toList());
            for (int i = 0; i < fieldNames.size(); i++) {
                fields.put(fieldNames.get(i), convertValueForJson(udtValue.getObject(i)));
            }
            return fields;
        }
        if (value instanceof TupleValue) {
            TupleValue tupleValue = (TupleValue) value;
            List<Object> elements = new ArrayList<>();
            int size = tupleValue.getType().getComponentTypes().size();
            for (int i = 0; i < size; i++) {
                elements.add(convertValueForJson(tupleValue.getObject(i)));
            }
            return elements;
        }
        if (value instanceof ByteBuffer) {
            // Convert to base64 for JSON
            byte[] bytes = convertBlob((ByteBuffer) value);
            return java.util.Base64.getEncoder().encodeToString(bytes);
        }
        if (value instanceof Instant) {
            return ((Instant) value).toString();
        }
        if (value instanceof UUID || value instanceof BigInteger || value instanceof BigDecimal
                || value instanceof InetAddress || value instanceof CqlDuration) {
            return value.toString();
        }
        return value;
    }
}
