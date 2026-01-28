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
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.sql.Array;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.util.PGobject;

import com.datastax.cdm.schema.PostgresTypeMapper.PostgresType;
import com.datastax.oss.driver.api.core.data.CqlDuration;
import com.datastax.oss.driver.api.core.type.DataTypes;

public class PostgresTypeMapperTest {

    private PostgresTypeMapper typeMapper;

    @BeforeEach
    public void setup() {
        typeMapper = new PostgresTypeMapper();
    }

    // ========== Type Mapping Tests ==========

    @Test
    public void mapCassandraType_text() {
        PostgresType type = typeMapper.mapCassandraType(DataTypes.TEXT);

        assertEquals("text", type.getTypeName());
        assertEquals(Types.VARCHAR, type.getSqlType());
        assertEquals(String.class, type.getJavaClass());
        assertFalse(type.isArray());
        assertFalse(type.isJsonb());
    }

    @Test
    public void mapCassandraType_ascii() {
        PostgresType type = typeMapper.mapCassandraType(DataTypes.ASCII);

        assertEquals("text", type.getTypeName());
        assertEquals(Types.VARCHAR, type.getSqlType());
    }

    @Test
    public void mapCassandraType_int() {
        PostgresType type = typeMapper.mapCassandraType(DataTypes.INT);

        assertEquals("integer", type.getTypeName());
        assertEquals(Types.INTEGER, type.getSqlType());
        assertEquals(Integer.class, type.getJavaClass());
    }

    @Test
    public void mapCassandraType_bigint() {
        PostgresType type = typeMapper.mapCassandraType(DataTypes.BIGINT);

        assertEquals("bigint", type.getTypeName());
        assertEquals(Types.BIGINT, type.getSqlType());
        assertEquals(Long.class, type.getJavaClass());
    }

    @Test
    public void mapCassandraType_smallint() {
        PostgresType type = typeMapper.mapCassandraType(DataTypes.SMALLINT);

        assertEquals("smallint", type.getTypeName());
        assertEquals(Types.SMALLINT, type.getSqlType());
        assertEquals(Short.class, type.getJavaClass());
    }

    @Test
    public void mapCassandraType_tinyint() {
        PostgresType type = typeMapper.mapCassandraType(DataTypes.TINYINT);

        assertEquals("smallint", type.getTypeName()); // Maps to smallint
        assertEquals(Types.SMALLINT, type.getSqlType());
    }

    @Test
    public void mapCassandraType_float() {
        PostgresType type = typeMapper.mapCassandraType(DataTypes.FLOAT);

        assertEquals("real", type.getTypeName());
        assertEquals(Types.REAL, type.getSqlType());
        assertEquals(Float.class, type.getJavaClass());
    }

    @Test
    public void mapCassandraType_double() {
        PostgresType type = typeMapper.mapCassandraType(DataTypes.DOUBLE);

        assertEquals("double precision", type.getTypeName());
        assertEquals(Types.DOUBLE, type.getSqlType());
        assertEquals(Double.class, type.getJavaClass());
    }

    @Test
    public void mapCassandraType_decimal() {
        PostgresType type = typeMapper.mapCassandraType(DataTypes.DECIMAL);

        assertEquals("numeric", type.getTypeName());
        assertEquals(Types.NUMERIC, type.getSqlType());
        assertEquals(BigDecimal.class, type.getJavaClass());
    }

    @Test
    public void mapCassandraType_varint() {
        PostgresType type = typeMapper.mapCassandraType(DataTypes.VARINT);

        assertEquals("numeric", type.getTypeName());
        assertEquals(Types.NUMERIC, type.getSqlType());
        assertEquals(BigDecimal.class, type.getJavaClass());
    }

    @Test
    public void mapCassandraType_boolean() {
        PostgresType type = typeMapper.mapCassandraType(DataTypes.BOOLEAN);

        assertEquals("boolean", type.getTypeName());
        assertEquals(Types.BOOLEAN, type.getSqlType());
        assertEquals(Boolean.class, type.getJavaClass());
    }

    @Test
    public void mapCassandraType_uuid() {
        PostgresType type = typeMapper.mapCassandraType(DataTypes.UUID);

        assertEquals("uuid", type.getTypeName());
        assertEquals(Types.OTHER, type.getSqlType());
        assertEquals(UUID.class, type.getJavaClass());
    }

    @Test
    public void mapCassandraType_timeuuid() {
        PostgresType type = typeMapper.mapCassandraType(DataTypes.TIMEUUID);

        assertEquals("uuid", type.getTypeName());
        assertEquals(Types.OTHER, type.getSqlType());
    }

    @Test
    public void mapCassandraType_timestamp() {
        PostgresType type = typeMapper.mapCassandraType(DataTypes.TIMESTAMP);

        assertEquals("timestamp with time zone", type.getTypeName());
        assertEquals(Types.TIMESTAMP_WITH_TIMEZONE, type.getSqlType());
        assertEquals(OffsetDateTime.class, type.getJavaClass());
    }

    @Test
    public void mapCassandraType_date() {
        PostgresType type = typeMapper.mapCassandraType(DataTypes.DATE);

        assertEquals("date", type.getTypeName());
        assertEquals(Types.DATE, type.getSqlType());
        assertEquals(LocalDate.class, type.getJavaClass());
    }

    @Test
    public void mapCassandraType_time() {
        PostgresType type = typeMapper.mapCassandraType(DataTypes.TIME);

        assertEquals("time", type.getTypeName());
        assertEquals(Types.TIME, type.getSqlType());
        assertEquals(LocalTime.class, type.getJavaClass());
    }

    @Test
    public void mapCassandraType_duration() {
        PostgresType type = typeMapper.mapCassandraType(DataTypes.DURATION);

        assertEquals("interval", type.getTypeName());
        assertEquals(Types.OTHER, type.getSqlType());
        assertEquals(PGobject.class, type.getJavaClass());
    }

    @Test
    public void mapCassandraType_blob() {
        PostgresType type = typeMapper.mapCassandraType(DataTypes.BLOB);

        assertEquals("bytea", type.getTypeName());
        assertEquals(Types.BINARY, type.getSqlType());
        assertEquals(byte[].class, type.getJavaClass());
    }

    @Test
    public void mapCassandraType_inet() {
        PostgresType type = typeMapper.mapCassandraType(DataTypes.INET);

        assertEquals("inet", type.getTypeName());
        assertEquals(Types.OTHER, type.getSqlType());
        assertEquals(PGobject.class, type.getJavaClass());
    }

    @Test
    public void mapCassandraType_list() {
        PostgresType type = typeMapper.mapCassandraType(DataTypes.listOf(DataTypes.TEXT));

        assertEquals("text[]", type.getTypeName());
        assertEquals(Types.ARRAY, type.getSqlType());
        assertEquals(Array.class, type.getJavaClass());
        assertTrue(type.isArray());
        assertEquals("text", type.getArrayElementType());
    }

    @Test
    public void mapCassandraType_set() {
        PostgresType type = typeMapper.mapCassandraType(DataTypes.setOf(DataTypes.INT));

        assertEquals("integer[]", type.getTypeName());
        assertEquals(Types.ARRAY, type.getSqlType());
        assertTrue(type.isArray());
        assertEquals("integer", type.getArrayElementType());
    }

    @Test
    public void mapCassandraType_map() {
        PostgresType type = typeMapper.mapCassandraType(DataTypes.mapOf(DataTypes.TEXT, DataTypes.TEXT));

        assertEquals("jsonb", type.getTypeName());
        assertEquals(Types.OTHER, type.getSqlType());
        assertEquals(PGobject.class, type.getJavaClass());
        assertTrue(type.isJsonb());
    }

    @Test
    public void mapCassandraType_null_defaultsToText() {
        PostgresType type = typeMapper.mapCassandraType(null);

        assertEquals("text", type.getTypeName());
        assertEquals(Types.VARCHAR, type.getSqlType());
    }

    // ========== Value Conversion Tests ==========

    @Test
    public void convertValue_null_returnsNull() throws SQLException {
        assertNull(typeMapper.convertValue(null, DataTypes.TEXT, null));
    }

    @Test
    public void convertValue_string_passesThrough() throws SQLException {
        String value = "test string";
        Object result = typeMapper.convertValue(value, DataTypes.TEXT, null);
        assertEquals(value, result);
    }

    @Test
    public void convertValue_integer_passesThrough() throws SQLException {
        Integer value = 42;
        Object result = typeMapper.convertValue(value, DataTypes.INT, null);
        assertEquals(value, result);
    }

    @Test
    public void convertValue_blob_convertsToByteArray() throws SQLException {
        byte[] bytes = "test".getBytes();
        ByteBuffer buffer = ByteBuffer.wrap(bytes);

        Object result = typeMapper.convertValue(buffer, DataTypes.BLOB, null);

        assertTrue(result instanceof byte[]);
        assertArrayEquals(bytes, (byte[]) result);
    }

    @Test
    public void convertValue_timestamp_convertsToOffsetDateTime() throws SQLException {
        Instant instant = Instant.parse("2024-01-15T10:30:00Z");

        Object result = typeMapper.convertValue(instant, DataTypes.TIMESTAMP, null);

        assertTrue(result instanceof OffsetDateTime);
        OffsetDateTime odt = (OffsetDateTime) result;
        assertEquals(ZoneOffset.UTC, odt.getOffset());
        assertEquals(instant, odt.toInstant());
    }

    @Test
    public void convertValue_bigInteger_convertsToBigDecimal() throws SQLException {
        BigInteger bigInt = new BigInteger("123456789012345678901234567890");

        Object result = typeMapper.convertValue(bigInt, DataTypes.VARINT, null);

        assertTrue(result instanceof BigDecimal);
        assertEquals(new BigDecimal(bigInt), result);
    }

    @Test
    public void convertValue_list_convertsToArray() throws SQLException {
        List<String> list = Arrays.asList("a", "b", "c");
        Connection mockConn = mock(Connection.class);
        Array mockArray = mock(Array.class);
        when(mockConn.createArrayOf(eq("text"), eq(new Object[] { "a", "b", "c" }))).thenReturn(mockArray);

        Object result = typeMapper.convertValue(list, DataTypes.listOf(DataTypes.TEXT), mockConn);

        assertEquals(mockArray, result);
    }

    @Test
    public void convertValue_set_convertsToArray() throws SQLException {
        Set<Integer> set = new HashSet<>(Arrays.asList(1, 2, 3));
        Connection mockConn = mock(Connection.class);
        Array mockArray = mock(Array.class);
        when(mockConn.createArrayOf(anyString(), eq(new Object[] { 1, 2, 3 }))).thenReturn(mockArray);

        Object result = typeMapper.convertValue(set, DataTypes.setOf(DataTypes.INT), mockConn);

        assertEquals(mockArray, result);
    }

    @Test
    public void convertValue_map_convertsToJsonb() throws SQLException {
        Map<String, Integer> map = new HashMap<>();
        map.put("one", 1);
        map.put("two", 2);

        Object result = typeMapper.convertValue(map, DataTypes.mapOf(DataTypes.TEXT, DataTypes.INT), null);

        assertTrue(result instanceof PGobject);
        PGobject pgObj = (PGobject) result;
        assertEquals("jsonb", pgObj.getType());
        String json = pgObj.getValue();
        assertTrue(json.contains("\"one\""));
        assertTrue(json.contains("\"two\""));
    }

    // ========== Direct Conversion Method Tests ==========

    @Test
    public void convertBlob_null_returnsNull() {
        assertNull(typeMapper.convertBlob(null));
    }

    @Test
    public void convertBlob_convertsCorrectly() {
        byte[] original = "test data".getBytes();
        ByteBuffer buffer = ByteBuffer.wrap(original);

        byte[] result = typeMapper.convertBlob(buffer);

        assertArrayEquals(original, result);
    }

    @Test
    public void convertTimestamp_null_returnsNull() {
        assertNull(typeMapper.convertTimestamp(null));
    }

    @Test
    public void convertTimestamp_convertsCorrectly() {
        Instant instant = Instant.parse("2024-06-15T12:30:45.123Z");

        OffsetDateTime result = typeMapper.convertTimestamp(instant);

        assertEquals(ZoneOffset.UTC, result.getOffset());
        assertEquals(2024, result.getYear());
        assertEquals(6, result.getMonthValue());
        assertEquals(15, result.getDayOfMonth());
    }

    @Test
    public void convertDuration_null_returnsNull() throws SQLException {
        assertNull(typeMapper.convertDuration(null));
    }

    @Test
    public void convertDuration_convertsCorrectly() throws SQLException {
        CqlDuration duration = CqlDuration.newInstance(2, 10, 3600000000000L); // 2 months, 10 days, 1 hour

        PGobject result = typeMapper.convertDuration(duration);

        assertNotNull(result);
        assertEquals("interval", result.getType());
        String value = result.getValue();
        assertTrue(value.contains("2 months"));
        assertTrue(value.contains("10 days"));
    }

    @Test
    public void convertInet_null_returnsNull() throws SQLException {
        assertNull(typeMapper.convertInet(null));
    }

    @Test
    public void convertInet_convertsCorrectly() throws SQLException, UnknownHostException {
        InetAddress inet = InetAddress.getByName("192.168.1.100");

        PGobject result = typeMapper.convertInet(inet);

        assertNotNull(result);
        assertEquals("inet", result.getType());
        assertEquals("192.168.1.100", result.getValue());
    }

    @Test
    public void convertVector_null_returnsNull() throws SQLException {
        assertNull(typeMapper.convertVector(null));
    }

    @Test
    public void convertVector_convertsCorrectly() throws SQLException {
        List<Float> vector = Arrays.asList(1.0f, 2.5f, 3.7f);

        PGobject result = typeMapper.convertVector(vector);

        assertNotNull(result);
        assertEquals("vector", result.getType());
        assertEquals("[1.0,2.5,3.7]", result.getValue());
    }

    @Test
    public void convertMap_convertsToJsonb() throws SQLException {
        Map<String, Object> map = new HashMap<>();
        map.put("name", "test");
        map.put("count", 42);
        map.put("active", true);

        PGobject result = typeMapper.convertMap(map);

        assertNotNull(result);
        assertEquals("jsonb", result.getType());
        String json = result.getValue();
        assertTrue(json.contains("\"name\""));
        assertTrue(json.contains("\"test\""));
        assertTrue(json.contains("\"count\""));
        assertTrue(json.contains("42"));
    }

    @Test
    public void convertMap_handlesNestedMap() throws SQLException {
        Map<String, Object> inner = new HashMap<>();
        inner.put("nested", "value");

        Map<String, Object> outer = new HashMap<>();
        outer.put("inner", inner);

        PGobject result = typeMapper.convertMap(outer);

        assertNotNull(result);
        assertEquals("jsonb", result.getType());
        String json = result.getValue();
        assertTrue(json.contains("\"nested\""));
        assertTrue(json.contains("\"value\""));
    }

    @Test
    public void convertList_convertsToArray() throws SQLException {
        List<String> list = Arrays.asList("x", "y", "z");
        Connection mockConn = mock(Connection.class);
        Array mockArray = mock(Array.class);
        when(mockConn.createArrayOf("text", new Object[] { "x", "y", "z" })).thenReturn(mockArray);

        Array result = typeMapper.convertList(list, mockConn, DataTypes.TEXT);

        assertEquals(mockArray, result);
    }

    @Test
    public void convertSet_convertsToArray() throws SQLException {
        Set<Long> set = new HashSet<>(Arrays.asList(100L, 200L));
        Connection mockConn = mock(Connection.class);
        Array mockArray = mock(Array.class);
        when(mockConn.createArrayOf(eq("bigint"), eq(new Object[] { 100L, 200L }))).thenReturn(mockArray);

        Array result = typeMapper.convertSet(set, mockConn, DataTypes.BIGINT);

        assertEquals(mockArray, result);
    }

    // ========== PostgresType Tests ==========

    @Test
    public void postgresType_toString() {
        PostgresType type = new PostgresType("text", Types.VARCHAR, String.class);

        String str = type.toString();

        assertTrue(str.contains("text"));
        assertTrue(str.contains(String.valueOf(Types.VARCHAR)));
    }

    @Test
    public void postgresType_arrayType() {
        PostgresType type = new PostgresType("integer[]", Types.ARRAY, Array.class, "integer");

        assertTrue(type.isArray());
        assertEquals("integer", type.getArrayElementType());
        assertFalse(type.isJsonb());
    }

    @Test
    public void postgresType_jsonbType() {
        PostgresType type = new PostgresType("jsonb", Types.OTHER, PGobject.class);

        assertTrue(type.isJsonb());
        assertFalse(type.isArray());
        assertNull(type.getArrayElementType());
    }
}
