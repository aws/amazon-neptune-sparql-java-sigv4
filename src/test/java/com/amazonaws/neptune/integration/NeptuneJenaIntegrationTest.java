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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@EnabledIfSystemProperty(named = "neptune.endpoint", matches = ".*")
class NeptuneJenaIntegrationTest {

    private HttpClient signingClient;
    private String testGraphUri;
    private final String regionName = System.getProperty(
            "aws.region",
            "us-west-1"
    );
    private final String neptuneEndpoint = System.getProperty(
            "neptune.endpoint",
            "https://xxxx.xxxx.us-west-1.neptune.amazonaws.com:8182/"
    );
    private DefaultCredentialsProvider credentialsProvider;


    @BeforeEach
    void setUp() {
        assertNotNull(neptuneEndpoint, "Neptune endpoint must be provided via -Dneptune.endpoint=<endpoint>");
        assertNotNull(regionName, "AWS region must be provided via -Daws.region=<region>");

        credentialsProvider = DefaultCredentialsProvider.create();

        testGraphUri = "http://neptune.aws.com/ontology/testing/" + UUID.randomUUID();
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

//    @Test
    void testInsertAndQueryWithJena() {

        String insertQuery = getInsertQuery(testGraphUri);

        signingClient = new AwsSigningHttpClient(
                "neptune-db",
                Region.of(regionName),
                credentialsProvider,
                insertQuery
        );

        try (RDFConnection conn = RDFConnectionRemote.create()
                .httpClient(signingClient)
                .destination(neptuneEndpoint)
                .queryEndpoint("sparql")
                .updateEndpoint("sparql")
                .build()) {

            conn.update(insertQuery);
            System.out.println("✓ Insert completed successfully");

            final int[] resultCount = {0};

            conn.querySelect(getSelectQuery(testGraphUri), result -> {
                resultCount[0]++;
            });

            assertEquals(1, resultCount[0], "Should find exactly 2 results");
            System.out.println("✓ Query completed successfully");

        }

    }

}