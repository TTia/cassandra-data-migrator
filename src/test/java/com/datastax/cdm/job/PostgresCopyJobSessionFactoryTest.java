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
package com.datastax.cdm.job;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.datastax.cdm.properties.KnownProperties;
import com.datastax.cdm.properties.PropertyHelper;

public class PostgresCopyJobSessionFactoryTest {

    @Mock
    private PropertyHelper propertyHelper;

    @BeforeEach
    public void setup() {
        MockitoAnnotations.openMocks(this);
        // Reset factory state between tests
        PostgresCopyJobSessionFactory.shutdown();
    }

    @Test
    public void isPostgresTarget_postgres_returnsTrue() {
        when(propertyHelper.getString(KnownProperties.TARGET_TYPE)).thenReturn("postgres");
        assertTrue(PostgresCopyJobSessionFactory.isPostgresTarget(propertyHelper));
    }

    @Test
    public void isPostgresTarget_postgresql_returnsTrue() {
        when(propertyHelper.getString(KnownProperties.TARGET_TYPE)).thenReturn("postgresql");
        assertTrue(PostgresCopyJobSessionFactory.isPostgresTarget(propertyHelper));
    }

    @Test
    public void isPostgresTarget_POSTGRES_caseInsensitive_returnsTrue() {
        when(propertyHelper.getString(KnownProperties.TARGET_TYPE)).thenReturn("POSTGRES");
        assertTrue(PostgresCopyJobSessionFactory.isPostgresTarget(propertyHelper));
    }

    @Test
    public void isPostgresTarget_cassandra_returnsFalse() {
        when(propertyHelper.getString(KnownProperties.TARGET_TYPE)).thenReturn("cassandra");
        assertFalse(PostgresCopyJobSessionFactory.isPostgresTarget(propertyHelper));
    }

    @Test
    public void isPostgresTarget_null_returnsFalse() {
        when(propertyHelper.getString(KnownProperties.TARGET_TYPE)).thenReturn(null);
        assertFalse(PostgresCopyJobSessionFactory.isPostgresTarget(propertyHelper));
    }

    @Test
    public void isPostgresTarget_empty_returnsFalse() {
        when(propertyHelper.getString(KnownProperties.TARGET_TYPE)).thenReturn("");
        assertFalse(PostgresCopyJobSessionFactory.isPostgresTarget(propertyHelper));
    }

    @Test
    public void shutdown_nullFactory_noError() {
        // Should not throw even when nothing is initialized
        PostgresCopyJobSessionFactory.shutdown();
    }

    @Test
    public void getConnectionFactory_beforeInit_returnsNull() {
        PostgresCopyJobSessionFactory factory = new PostgresCopyJobSessionFactory();
        assertNull(factory.getConnectionFactory());
    }
}
