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
package com.datastax.cdm.integration;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.util.PGobject;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import com.datastax.cdm.schema.PostgresTypeMapper;
import com.datastax.oss.driver.api.core.type.DataTypes;

/**
 * Checks that the values PostgresTypeMapper produces are accepted by a real PostgreSQL and read back intact. The end to
 * end copy and diff are covered by PostgresJobSessionTest.
 */
@Testcontainers(disabledWithoutDocker = true)
public class PostgresTypeMapperIntegrationTest {

    private static final String TIMESCALE_IMAGE = "timescale/timescaledb:latest-pg16";

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(
            DockerImageName.parse(TIMESCALE_IMAGE).asCompatibleSubstituteFor("postgres")).withDatabaseName("testdb")
                    .withUsername("testuser").withPassword("testpass");

    private static Connection connection;
    private static PostgresTypeMapper typeMapper;

    @BeforeAll
    static void setup() throws SQLException {
        connection = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
        typeMapper = new PostgresTypeMapper();

        // Create test tables
        createTestTables();
    }

    @AfterAll
    static void teardown() throws SQLException {
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }

    private static void createTestTables() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            // Collection types table
            stmt.execute("CREATE TABLE IF NOT EXISTS collection_types (" + "id UUID PRIMARY KEY, " + "list_col TEXT[], "
                    + "set_col INTEGER[], " + "map_col JSONB)");

            // Complex types table
            stmt.execute("CREATE TABLE IF NOT EXISTS complex_types (" + "id UUID PRIMARY KEY, " + "udt_col JSONB, "
                    + "blob_col BYTEA, " + "inet_col INET)");
        }
    }

    // ==================== Collection Type Tests ====================

    @Test
    void testListToArray() throws SQLException {
        UUID id = UUID.randomUUID();
        List<String> list = Arrays.asList("a", "b", "c");

        // Convert using type mapper
        java.sql.Array array = typeMapper.convertList(list, connection, DataTypes.TEXT);

        // Insert
        String insertSql = "INSERT INTO collection_types (id, list_col) VALUES (?, ?)";
        try (PreparedStatement pstmt = connection.prepareStatement(insertSql)) {
            pstmt.setObject(1, id);
            pstmt.setArray(2, array);
            pstmt.executeUpdate();
        }

        // Select and verify
        String selectSql = "SELECT list_col FROM collection_types WHERE id = ?";
        try (PreparedStatement pstmt = connection.prepareStatement(selectSql)) {
            pstmt.setObject(1, id);
            try (ResultSet rs = pstmt.executeQuery()) {
                assertTrue(rs.next());
                java.sql.Array resultArray = rs.getArray("list_col");
                String[] resultValues = (String[]) resultArray.getArray();
                assertArrayEquals(list.toArray(), resultValues);
            }
        }
    }

    @Test
    void testSetToArray() throws SQLException {
        UUID id = UUID.randomUUID();
        java.util.Set<Integer> set = new java.util.HashSet<>(Arrays.asList(1, 2, 3));

        // Convert using type mapper
        java.sql.Array array = typeMapper.convertSet(set, connection, DataTypes.INT);

        // Insert
        String insertSql = "INSERT INTO collection_types (id, set_col) VALUES (?, ?)";
        try (PreparedStatement pstmt = connection.prepareStatement(insertSql)) {
            pstmt.setObject(1, id);
            pstmt.setArray(2, array);
            pstmt.executeUpdate();
        }

        // Select and verify
        String selectSql = "SELECT set_col FROM collection_types WHERE id = ?";
        try (PreparedStatement pstmt = connection.prepareStatement(selectSql)) {
            pstmt.setObject(1, id);
            try (ResultSet rs = pstmt.executeQuery()) {
                assertTrue(rs.next());
                java.sql.Array resultArray = rs.getArray("set_col");
                Integer[] resultValues = (Integer[]) resultArray.getArray();
                assertEquals(3, resultValues.length);
            }
        }
    }

    @Test
    void testMapToJsonb() throws SQLException {
        UUID id = UUID.randomUUID();
        Map<String, Object> map = new HashMap<>();
        map.put("key1", "value1");
        map.put("key2", 42);
        map.put("nested", Map.of("inner", "data"));

        // Convert using type mapper
        PGobject jsonb = typeMapper.convertMap(map);

        // Insert
        String insertSql = "INSERT INTO collection_types (id, map_col) VALUES (?, ?)";
        try (PreparedStatement pstmt = connection.prepareStatement(insertSql)) {
            pstmt.setObject(1, id);
            pstmt.setObject(2, jsonb);
            pstmt.executeUpdate();
        }

        // Select and verify
        String selectSql = "SELECT map_col FROM collection_types WHERE id = ?";
        try (PreparedStatement pstmt = connection.prepareStatement(selectSql)) {
            pstmt.setObject(1, id);
            try (ResultSet rs = pstmt.executeQuery()) {
                assertTrue(rs.next());
                String jsonStr = rs.getString("map_col");
                assertNotNull(jsonStr);
                assertTrue(jsonStr.contains("key1"));
                assertTrue(jsonStr.contains("value1"));
                assertTrue(jsonStr.contains("nested"));
            }
        }
    }

    // ==================== Complex Type Tests ====================

    @Test
    void testBlobToBytea() throws SQLException {
        UUID id = UUID.randomUUID();
        byte[] blobData = "Binary test data".getBytes();
        java.nio.ByteBuffer buffer = java.nio.ByteBuffer.wrap(blobData);

        // Convert using type mapper
        byte[] converted = typeMapper.convertBlob(buffer);

        // Insert
        String insertSql = "INSERT INTO complex_types (id, blob_col) VALUES (?, ?)";
        try (PreparedStatement pstmt = connection.prepareStatement(insertSql)) {
            pstmt.setObject(1, id);
            pstmt.setBytes(2, converted);
            pstmt.executeUpdate();
        }

        // Select and verify
        String selectSql = "SELECT blob_col FROM complex_types WHERE id = ?";
        try (PreparedStatement pstmt = connection.prepareStatement(selectSql)) {
            pstmt.setObject(1, id);
            try (ResultSet rs = pstmt.executeQuery()) {
                assertTrue(rs.next());
                byte[] resultBytes = rs.getBytes("blob_col");
                assertArrayEquals(blobData, resultBytes);
            }
        }
    }

    @Test
    void testInetType() throws SQLException {
        UUID id = UUID.randomUUID();
        java.net.InetAddress inet = java.net.InetAddress.getLoopbackAddress();

        // Convert using type mapper
        PGobject pgInet = typeMapper.convertInet(inet);

        // Insert
        String insertSql = "INSERT INTO complex_types (id, inet_col) VALUES (?, ?)";
        try (PreparedStatement pstmt = connection.prepareStatement(insertSql)) {
            pstmt.setObject(1, id);
            pstmt.setObject(2, pgInet);
            pstmt.executeUpdate();
        }

        // Select and verify
        String selectSql = "SELECT inet_col FROM complex_types WHERE id = ?";
        try (PreparedStatement pstmt = connection.prepareStatement(selectSql)) {
            pstmt.setObject(1, id);
            try (ResultSet rs = pstmt.executeQuery()) {
                assertTrue(rs.next());
                Object inetResult = rs.getObject("inet_col");
                assertNotNull(inetResult);
            }
        }
    }
}
