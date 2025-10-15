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

package com.amazonaws.neptune.integration;

import com.amazonaws.neptune.client.jena.AwsSigningHttpClient;
import org.apache.jena.atlas.web.HttpException;
import org.apache.jena.rdfconnection.RDFConnection;
import org.apache.jena.rdfconnection.RDFConnectionRemote;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;

import java.net.http.HttpClient;
import java.util.UUID;

import static com.amazonaws.neptune.NeptuneConnectionIntegrationTestBase.*;
import static org.junit.jupiter.api.Assertions.*;

@EnabledIfSystemProperty(named = "neptune.endpoint", matches = ".*")
class NeptuneJenaIntegrationTest {

    private HttpClient signingClient;
    private String testGraphUri;
    private String testGraphUri2;
    private final String regionName = System.getProperty(
            "aws.region",
            "us-west-1"
    );
    private final String neptuneEndpoint = System.getProperty(
            "neptune.endpoint",
            "https://playground.cluster-cfk6p1jkvase.us-west-1.neptune.amazonaws.com:8182/"
    );
    private DefaultCredentialsProvider credentialsProvider;


    @BeforeEach
    void setUp() {
        assertNotNull(neptuneEndpoint, "Neptune endpoint must be provided via -Dneptune.endpoint=<endpoint>");
        assertNotNull(regionName, "AWS region must be provided via -Daws.region=<region>");

        credentialsProvider = DefaultCredentialsProvider.builder().build();

        testGraphUri = "http://neptune.aws.com/ontology/testing/" + UUID.randomUUID();
        testGraphUri2 = "http://neptune.aws.com/ontology/testing/" + UUID.randomUUID();
    }

    @AfterEach
    void cleanup() {

        String deleteQuery = getClearQuery(testGraphUri);

        signingClient = new AwsSigningHttpClient(
                "neptune-db",
                Region.of(regionName),
                credentialsProvider,
                deleteQuery
        );

        try (RDFConnection conn = RDFConnectionRemote.create()
                .httpClient(signingClient)
                .destination(neptuneEndpoint)
                .updateEndpoint("sparql")
                .build()) {

            conn.update(deleteQuery);
            System.out.println("✓ Cleanup completed");
        } catch (Exception e) {
            System.err.println("Cleanup failed: " + e.getMessage());
        }

    }

    /**
     * Tests Neptune SPARQL operations with Jena, demonstrating the critical requirement
     * that the request body (SPARQL query) must be provided when creating the signing client.
     * <p>
     * <strong>AWS Signature V4 Body Requirement:</strong>
     * AWS Signature Version 4 requires the request body to be included in the signature
     * calculation to ensure request integrity and prevent tampering. The signature is
     * computed using a hash of the request body along with other request components
     * (headers, URI, method, etc.). This means:
     * <ul>
     *   <li>The exact request body must be known at signature creation time</li>
     *   <li>Any modification to the body after signing would invalidate the signature</li>
     *   <li>Neptune will reject requests where the body doesn't match the signature</li>
     * </ul>
     * <p>
     * <strong>Why This Matters for SPARQL:</strong>
     * SPARQL queries are sometimes sent as HTTP POST requests with the query in the request body.
     * Since AWS Signature V4 includes the body hash in the signature calculation,
     * we must provide the exact SPARQL query text when creating the AwsSigningHttpClient.
     * This ensures the signature matches what Neptune expects when it validates the request.
     * <p>
     * This test verifies that the signing process works correctly by:
     * <ol>
     *   <li>Creating a signing client with the INSERT query body</li>
     *   <li>Executing the INSERT operation (which must match the signed body)</li>
     *   <li>Verifying the data was inserted by querying it back</li>
     * </ol>
     */
    @Test
    void testInsertAndQueryWithJena() {
        // Generate the SPARQL INSERT query that will be sent in the request body
        String insertQuery = getInsertQuery(testGraphUri);

        // CRITICAL: The query string itself must be provided to the signing client because
        // AWS Signature V4 requires the request body to be included in signature calculation.
        // The signature includes a hash of the request body to ensure integrity.
        // A new signingClient must be created for every request that contains a body. (POST, PUT)
        //
        signingClient = new AwsSigningHttpClient(
                "neptune-db",
                Region.of(regionName),
                credentialsProvider,
                insertQuery  // This exact query body will be used for signature calculation
        );

        try (RDFConnection conn = RDFConnectionRemote.create()
                .httpClient(signingClient)
                .destination(neptuneEndpoint)
                .queryEndpoint("sparql")
                .updateEndpoint("sparql")
                .build()) {

            // Execute the INSERT - the request body must match what was used for signing
            conn.update(insertQuery);
            System.out.println("✓ Insert completed successfully");

            final int[] resultCount = {0};

            // Query the inserted data to verify the operation succeeded
            // As this SELECT query uses GET, there is no request body.
            conn.querySelect(getSelectQuery(testGraphUri), result -> {
                resultCount[0]++;
            });

            assertEquals(1, resultCount[0], "Should find exactly 1 result");
            System.out.println("✓ Query completed successfully");
        }
    }

    @Test
    void testSecondInsertRequiresNewClientFroCorrectSignatureCreation() {
        // Generate the SPARQL INSERT query that will be sent in the request body
        String insertQuery = getInsertQuery(testGraphUri);
        String insertQuery2 = getInsertQuery(testGraphUri2);

        signingClient = new AwsSigningHttpClient(
                "neptune-db",
                Region.of(regionName),
                credentialsProvider,
                insertQuery  // This exact query body will be used for signature calculation
        );

        try (RDFConnection conn = RDFConnectionRemote.create()
                .httpClient(signingClient)
                .destination(neptuneEndpoint)
                .queryEndpoint("sparql")
                .updateEndpoint("sparql")
                .build()) {

            // Execute the INSERT - the request body must match what was used for signing
            conn.update(insertQuery);
            System.out.println("✓ Insert completed successfully");
            conn.update(insertQuery2);
            fail("Should throw exception");
        } catch (HttpException hje) {
            assertTrue(hje.getMessage().contains("403 - Forbidden"));
        }

        HttpClient signingClient2 = new AwsSigningHttpClient(
                "neptune-db",
                Region.of(regionName),
                credentialsProvider,
                insertQuery2
        );
        try (RDFConnection conn = RDFConnectionRemote.create()
                .httpClient(signingClient2)
                .destination(neptuneEndpoint)
                .queryEndpoint("sparql")
                .updateEndpoint("sparql")
                .build()) {

            // Execute the INSERT - the request body must match what was used for signing
            conn.update(insertQuery2);
            System.out.println(
                    "✓ Insert 2 now completes successfully as new client and " +
                            "connection as auth header is correctly built using the body from insert 2."
            );
        }
    }


}