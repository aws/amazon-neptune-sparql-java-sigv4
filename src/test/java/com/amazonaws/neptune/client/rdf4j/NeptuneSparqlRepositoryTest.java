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
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

/**
 * Unit tests for NeptuneSparqlRepository.
 * <p>
 * Tests both authenticated and unauthenticated repository configurations.
 */
class NeptuneSparqlRepositoryTest {

    @Mock
    private AwsCredentialsProvider mockCredentialsProvider;

    private final String testEndpoint = "https://test-cluster.cluster-xyz.us-east-1.neptune.amazonaws.com:8182";
    private final String testRegion = "us-east-1";

    @Test
    void testUnauthenticatedRepositoryCreation() {
        // Test creating repository without authentication
        NeptuneSparqlRepository repository = new NeptuneSparqlRepository(testEndpoint);
        
        assertNotNull(repository);
        assertEquals(testEndpoint , repository.toString());
    }

    @Test
    void testAuthenticatedRepositoryCreation() throws NeptuneSigV4SignerException {
        MockitoAnnotations.openMocks(this);
        
        // Mock credentials provider
        when(mockCredentialsProvider.resolveCredentials())
                .thenReturn(AwsBasicCredentials.create("test-access-key", "test-secret-key"));

        // Test creating repository with authentication
        NeptuneSparqlRepository repository = new NeptuneSparqlRepository(
                testEndpoint,
                mockCredentialsProvider,
                testRegion
        );
        
        assertNotNull(repository);
        assertEquals(testEndpoint , repository.toString());
    }

    @Test
    void testNullEndpointThrowsException() {
        // Test that null endpoint throws appropriate exception
        assertThrows(Exception.class, () -> {
            new NeptuneSparqlRepository(null);
        });
    }

    @Test
    void testNullCredentialsProviderThrowsException() {
        // Test that null credentials provider throws exception for authenticated repository
        assertThrows(Exception.class, () -> {
            new NeptuneSparqlRepository(testEndpoint, null, testRegion);
        });
    }

    @Test
    void testNullRegionThrowsException() {
        MockitoAnnotations.openMocks(this);
        
        when(mockCredentialsProvider.resolveCredentials())
                .thenReturn(AwsBasicCredentials.create("test-access-key", "test-secret-key"));

        // Test that null region throws exception for authenticated repository
        assertThrows(Exception.class, () -> {
            new NeptuneSparqlRepository(testEndpoint, mockCredentialsProvider, null);
        });
    }

    @Test
    void testRepositoryInheritance() {
        // Test that repository properly extends SPARQLRepository
        NeptuneSparqlRepository repository = new NeptuneSparqlRepository(testEndpoint);
        
        assertTrue(repository instanceof org.eclipse.rdf4j.repository.sparql.SPARQLRepository);
    }

    @Test
    void testRepositoryConnection() {
        // Test that repository can provide connections
        NeptuneSparqlRepository repository = new NeptuneSparqlRepository(testEndpoint);
        
        assertDoesNotThrow(() -> {
            // This would normally connect to Neptune, but in unit tests we just verify no exceptions
            var connection = repository.getConnection();
            assertNotNull(connection);
            connection.close();
        });
    }

    @Test
    void testRepositoryInitialization() {
        NeptuneSparqlRepository repository = new NeptuneSparqlRepository(testEndpoint);
        
        // Test repository initialization
        assertDoesNotThrow(() -> {
            repository.init();
            assertTrue(repository.isInitialized());
        });
    }

    @Test
    void testRepositoryShutdown() {
        NeptuneSparqlRepository repository = new NeptuneSparqlRepository(testEndpoint);
        
        // Test repository shutdown
        assertDoesNotThrow(() -> {
            repository.init();
            repository.shutDown();
        });
    }
}