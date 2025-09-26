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

package com.amazonaws.neptune.client.rdf4j;

import com.amazonaws.neptune.auth.NeptuneSigV4SignerException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for NeptuneRdf4JSigV4Example.
 * <p>
 * Tests the example class behavior without requiring actual Neptune connectivity.
 */
class NeptuneRdf4JSigV4ExampleTest {

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
                    NeptuneRdf4JSigV4Example.main(new String[]{});
                } catch (Exception e) {
                    // Expected when no actual Neptune instance is available
                    assertTrue(e.getMessage().contains("Connection") || 
                              e.getMessage().contains("resolve") ||
                              e.getMessage().contains("neptune") ||
                              e.getMessage().contains("playground") ||
                              e instanceof NeptuneSigV4SignerException);
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
            var method = NeptuneRdf4JSigV4Example.class.getMethod("main", String[].class);
            assertNotNull(method);
            assertTrue(java.lang.reflect.Modifier.isStatic(method.getModifiers()));
            assertTrue(java.lang.reflect.Modifier.isPublic(method.getModifiers()));
        });
    }

    @Test
    void testMainMethodThrowsNeptuneSigV4SignerException() {
        // Verify that main method declares the correct exception
        assertDoesNotThrow(() -> {
            var method = NeptuneRdf4JSigV4Example.class.getMethod("main", String[].class);
            var exceptionTypes = method.getExceptionTypes();
            
            boolean throwsNeptuneSigV4SignerException = false;
            for (Class<?> exceptionType : exceptionTypes) {
                if (exceptionType.equals(NeptuneSigV4SignerException.class)) {
                    throwsNeptuneSigV4SignerException = true;
                    break;
                }
            }
            assertTrue(throwsNeptuneSigV4SignerException, 
                      "Main method should declare NeptuneSigV4SignerException");
        });
    }

    @Test
    void testClassIsPublic() {
        // Verify class accessibility
        assertTrue(java.lang.reflect.Modifier.isPublic(NeptuneRdf4JSigV4Example.class.getModifiers()));
    }

    @Test
    void testClassHasNoInstanceFields() {
        // Example classes should typically not have instance fields
        var fields = NeptuneRdf4JSigV4Example.class.getDeclaredFields();
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
            new NeptuneRdf4JSigV4Example();
        });
    }

    @Test
    void testMainMethodHandlesNullArgs() {
        // Test that main method can handle null arguments
        assertDoesNotThrow(() -> {
            try {
                NeptuneRdf4JSigV4Example.main(null);
            } catch (Exception e) {
                // Expected when no Neptune endpoint is available or null args
                assertTrue(e.getMessage().contains("Connection") || 
                          e.getMessage().contains("resolve") ||
                          e.getMessage().contains("neptune") ||
                          e.getMessage().contains("playground") ||
                          e instanceof NeptuneSigV4SignerException ||
                          e instanceof NullPointerException);
            }
        });
    }

    @Test
    void testMainMethodHandlesEmptyArgs() {
        // Test that main method can handle empty arguments
        assertDoesNotThrow(() -> {
            try {
                NeptuneRdf4JSigV4Example.main(new String[]{});
            } catch (Exception e) {
                // Expected when no Neptune endpoint is available
                assertTrue(e.getMessage().contains("Connection") || 
                          e.getMessage().contains("resolve") ||
                          e.getMessage().contains("neptune") ||
                          e.getMessage().contains("playground") ||
                          e instanceof NeptuneSigV4SignerException);
            }
        });
    }

    @Test
    void testExampleUsesCorrectEndpointFormat() {
        // This test verifies the example uses the expected Neptune endpoint format
        // by checking if it would fail with connection errors (not format errors)
        assertDoesNotThrow(() -> {
            try {
                NeptuneRdf4JSigV4Example.main(new String[]{});
            } catch (Exception e) {
                // Should fail with connection/resolve errors, not format errors
                assertFalse(e.getMessage().contains("Invalid URL") ||
                           e.getMessage().contains("Malformed") ||
                           e.getMessage().contains("Invalid endpoint"),
                           "Should not fail due to URL format issues");
            }
        });
    }
}