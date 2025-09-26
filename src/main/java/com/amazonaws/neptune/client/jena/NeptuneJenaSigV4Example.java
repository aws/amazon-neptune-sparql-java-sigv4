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

import org.apache.jena.rdfconnection.RDFConnection;
import org.apache.jena.rdfconnection.RDFConnectionRemote;
import org.apache.jena.rdfconnection.RDFConnectionRemoteBuilder;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;

import java.net.http.HttpClient;

/**
 * Example demonstrating how to connect to Amazon Neptune using Apache Jena with AWS Signature V4 authentication.
 * <p>
 * This example shows how to:
 * <ul>
 *   <li>Create an AWS signing HTTP client for Neptune authentication</li>
 *   <li>Configure a Jena RDF connection with the signing client</li>
 *   <li>Execute SPARQL queries against a Neptune database</li>
 * </ul>
 * <p>
 * The example uses the default AWS credentials provider, which will automatically
 * discover credentials from the environment, AWS profiles, or IAM roles.
 * 
 * @author AWS Neptune Team
 * @since 1.0
 */
public class NeptuneJenaSigV4Example {

    /**
     * Main method demonstrating Neptune connection with Jena and AWS Signature V4.
     * <p>
     * Replace the endpoint URL and region with your actual Neptune cluster details.
     * Ensure your AWS credentials are properly configured in your environment.
     * 
     * @param args command line arguments (not used)
     */
    public static void main(String... args) {
        // Neptune cluster configuration - replace with your actual values
        final String endpoint = "https://xxxxx.cluster-xxxx.us-west-1.neptune.amazonaws.com:8182";
        final String regionName = "us-west-1";
        
        // Use default AWS credentials provider (checks environment, profiles, IAM roles)
        final AwsCredentialsProvider awsCredentialsProvider = DefaultCredentialsProvider.builder().build();

        // Sample SPARQL query to retrieve triples
        String query = "SELECT * { ?s ?p ?o } LIMIT 100";

        // Create signing HTTP client for Neptune authentication
        HttpClient signingClient = new AwsSigningHttpClient(
                "neptune-db",  // AWS service name for Neptune
                Region.of(regionName),
                awsCredentialsProvider,
                query
        );

        // Build Jena RDF connection with the signing client
        RDFConnectionRemoteBuilder builder = RDFConnectionRemote.create()
                .httpClient(signingClient)
                .destination(endpoint)
                .queryEndpoint("sparql")    // Neptune SPARQL query endpoint
                .updateEndpoint("sparql");  // Neptune SPARQL update endpoint

        // Execute the query using try-with-resources for automatic connection cleanup
        try (RDFConnection conn = builder.build()) {
            System.out.println("> Printing query result: ");
            conn.querySelect(query, System.out::println);
        }
    }

}
