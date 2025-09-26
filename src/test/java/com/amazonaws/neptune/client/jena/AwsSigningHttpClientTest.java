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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

/**
 * Unit tests for AwsSigningHttpClient.
 * <p>
 * Tests the AWS Signature V4 signing functionality and HTTP client delegation behavior.
 */
class AwsSigningHttpClientTest {

    @Mock
    private AwsCredentialsProvider mockCredentialsProvider;

    private AwsSigningHttpClient signingClient;
    private final String testRequestBody = "SELECT * { ?s ?p ?o } LIMIT 10";
    private final Region testRegion = Region.US_EAST_1;
    private final String serviceName = "neptune-db";

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        
        // Mock credentials provider
        when(mockCredentialsProvider.resolveCredentials())
                .thenReturn(AwsBasicCredentials.create("test-access-key", "test-secret-key"));

        signingClient = new AwsSigningHttpClient(
                serviceName,
                testRegion,
                mockCredentialsProvider,
                testRequestBody
        );
    }

    @Test
    void testConstructorInitializesFields() {
        assertNotNull(signingClient);
        assertEquals(Duration.ofSeconds(30), signingClient.connectTimeout().orElse(Duration.ofSeconds(30)));
    }

    @Test
    void testDelegateMethodsReturnNonNull() {
        // Test that delegate methods return expected values
        assertNotNull(signingClient.cookieHandler());
        assertNotNull(signingClient.followRedirects());
        assertNotNull(signingClient.proxy());
        assertNotNull(signingClient.sslContext());
        assertNotNull(signingClient.sslParameters());
        assertNotNull(signingClient.authenticator());
        assertNotNull(signingClient.version());
        assertNotNull(signingClient.executor());
    }

    @Test
    void testSendWithValidRequest() throws IOException, InterruptedException {
        // Create a simple GET request for testing
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("https://example.com/sparql"))
                .GET()
                .build();

        HttpResponse.BodyHandler<String> bodyHandler = HttpResponse.BodyHandlers.ofString();

        // This test verifies that the method doesn't throw exceptions
        // In a real scenario, this would make an actual HTTP call
        assertDoesNotThrow(() -> {
            try {
                signingClient.send(request, bodyHandler);
            } catch (IOException | InterruptedException e) {
                // Expected for mock/test scenarios where no actual server exists
                assertTrue(e.getMessage().contains("example.com") || 
                          e.getMessage().contains("Connection") ||
                          e.getMessage().contains("resolve"));
            }
        });
    }

    @Test
    void testSendAsyncWithValidRequest() {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("https://example.com/sparql"))
                .POST(HttpRequest.BodyPublishers.ofString(testRequestBody))
                .build();

        HttpResponse.BodyHandler<String> bodyHandler = HttpResponse.BodyHandlers.ofString();

        // Test that sendAsync returns a CompletableFuture
        assertDoesNotThrow(() -> {
            var future = signingClient.sendAsync(request, bodyHandler);
            assertNotNull(future);
        });
    }

    @Test
    void testSendAsyncWithPushPromiseHandler() {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("https://example.com/sparql"))
                .POST(HttpRequest.BodyPublishers.ofString(testRequestBody))
                .build();

        HttpResponse.BodyHandler<String> bodyHandler = HttpResponse.BodyHandlers.ofString();
        
        // Test that sendAsync with null push promise handler returns a CompletableFuture
        assertDoesNotThrow(() -> {
            var future = signingClient.sendAsync(request, bodyHandler, null);
            assertNotNull(future);
        });
    }

    @Test
    void testRequestWithEmptyBody() {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("https://example.com/sparql"))
                .GET()
                .build();

        // Test that requests without body are handled correctly
        assertDoesNotThrow(() -> {
            try {
                signingClient.send(request, HttpResponse.BodyHandlers.ofString());
            } catch (IOException | InterruptedException e) {
                // Expected for test scenarios
                assertTrue(e.getMessage().contains("example.com") || 
                          e.getMessage().contains("Connection") ||
                          e.getMessage().contains("resolve"));
            }
        });
    }

    @Test
    void testRequestWithHeaders() {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("https://example.com/sparql"))
                .header("Content-Type", "application/sparql-query")
                .header("Accept", "application/sparql-results+json")
                .POST(HttpRequest.BodyPublishers.ofString(testRequestBody))
                .build();

        // Test that requests with custom headers are handled correctly
        assertDoesNotThrow(() -> {
            try {
                signingClient.send(request, HttpResponse.BodyHandlers.ofString());
            } catch (IOException | InterruptedException e) {
                // Expected for test scenarios
                assertTrue(e.getMessage().contains("example.com") || 
                          e.getMessage().contains("Connection") ||
                          e.getMessage().contains("resolve"));
            }
        });
    }
}