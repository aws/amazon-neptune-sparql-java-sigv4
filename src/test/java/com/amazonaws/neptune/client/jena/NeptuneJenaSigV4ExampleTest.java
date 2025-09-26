/*
 *   Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package com.amazonaws.neptune.client.jena;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for NeptuneJenaSigV4Example.
 * <p>
 * Tests the example class behavior without requiring actual Neptune connectivity.
 */
class NeptuneJenaSigV4ExampleTest {

    @Test
    @DisabledIfSystemProperty(named = "neptune.endpoint", matches = ".*")
    void testMainMethodWithoutNeptuneEndpoint() {
        // Capture system output
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        PrintStream originalOut = System.out;
        System.setOut(new PrintStream(outputStream));

        try {
            // This should fail gracefully when no Neptune endpoint is available
            assertDoesNotThrow(() -> {
                try {
                    NeptuneJenaSigV4Example.main();
                } catch (Exception e) {
                    // Expected when no actual Neptune instance is available
                    assertTrue(e.getMessage().contains("Connection") || 
                              e.getMessage().contains("resolve") ||
                              e.getMessage().contains("neptune") ||
                              e.getMessage().contains("playground"));
                }
            });
        } finally {
            System.setOut(originalOut);
        }
    }

    @Test
    void testMainMethodExists() {
        // Verify that the main method exists and is accessible
        assertDoesNotThrow(() -> {
            var method = NeptuneJenaSigV4Example.class.getMethod("main", String[].class);
            assertNotNull(method);
            assertTrue(java.lang.reflect.Modifier.isStatic(method.getModifiers()));
            assertTrue(java.lang.reflect.Modifier.isPublic(method.getModifiers()));
        });
    }

    @Test
    void testClassIsPublic() {
        // Verify class accessibility
        assertTrue(java.lang.reflect.Modifier.isPublic(NeptuneJenaSigV4Example.class.getModifiers()));
    }

    @Test
    void testClassHasNoInstanceFields() {
        // Example classes should typically not have instance fields
        var fields = NeptuneJenaSigV4Example.class.getDeclaredFields();
        for (var field : fields) {
            assertTrue(java.lang.reflect.Modifier.isStatic(field.getModifiers()) ||
                      java.lang.reflect.Modifier.isFinal(field.getModifiers()),
                      "Example class should not have mutable instance fields");
        }
    }

    @Test
    void testClassCanBeInstantiated() {
        // Verify the class can be instantiated (has default constructor)
        assertDoesNotThrow(() -> {
            new NeptuneJenaSigV4Example();
        });
    }

    @Test
    void testMainMethodHandlesNullArgs() {
        // Test that main method can handle null arguments
        assertDoesNotThrow(() -> {
            try {
                NeptuneJenaSigV4Example.main((String[]) null);
            } catch (Exception e) {
                // Expected when no Neptune endpoint is available
                assertTrue(e.getMessage().contains("Connection") || 
                          e.getMessage().contains("resolve") ||
                          e.getMessage().contains("neptune") ||
                          e.getMessage().contains("playground"));
            }
        });
    }

    @Test
    void testMainMethodHandlesEmptyArgs() {
        // Test that main method can handle empty arguments
        assertDoesNotThrow(() -> {
            try {
                NeptuneJenaSigV4Example.main(new String[]{});
            } catch (Exception e) {
                // Expected when no Neptune endpoint is available
                assertTrue(e.getMessage().contains("Connection") || 
                          e.getMessage().contains("resolve") ||
                          e.getMessage().contains("neptune") ||
                          e.getMessage().contains("playground"));
            }
        });
    }
}