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
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.TupleQueryResult;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;

/**
 * Example demonstrating how to connect to Amazon Neptune using Eclipse RDF4J with AWS Signature V4 authentication.
 * <p>
 * This example shows how to:
 * <ul>
 *   <li>Create a Neptune SPARQL repository with AWS authentication</li>
 *   <li>Execute SPARQL queries against a Neptune database</li>
 *   <li>Process query results using RDF4J APIs</li>
 * </ul>
 * <p>
 * The example uses the default AWS credentials provider, which will automatically
 * discover credentials from the environment, AWS profiles, or IAM roles.
 * 
 * @author AWS Neptune Team
 * @since 1.0
 */
public class NeptuneRdf4JSigV4Example {

    /**
     * Main method demonstrating Neptune connection with RDF4J and AWS Signature V4.
     * <p>
     * Requires two command line arguments: endpoint URL and region name.
     * Ensure your AWS credentials are properly configured in your environment.
     * <p>
     * Example usage:
     * {@code java NeptuneRdf4JSigV4Example https://my-cluster.cluster-xyz.us-west-2.neptune.amazonaws.com:8182/sparql us-west-2}
     * 
     * @param args command line arguments: [0] endpoint URL, [1] region name
     * @throws NeptuneSigV4SignerException if there's an error with AWS signature signing
     */
    public static void main(final String[] args) throws NeptuneSigV4SignerException {
        if (args.length < 2) {
            System.err.println("Usage: java NeptuneRdf4JSigV4Example <endpointUrl> <regionName>");
            System.exit(1);
        }
        
        final String endpointUrl = args[0];
        final String regionName = args[1];
        
        // Use default AWS credentials provider (checks environment, profiles, IAM roles)
        final AwsCredentialsProvider awsCredentialsProvider = DefaultCredentialsProvider.create();

        // Create Neptune SPARQL repository with AWS Signature V4 authentication
        final NeptuneSparqlRepository repository = new NeptuneSparqlRepository(
                endpointUrl,
                awsCredentialsProvider,
                regionName
        );

        // Sample SPARQL query to retrieve triples
        final String queryString = "SELECT ?s ?p ?o WHERE { ?s ?p ?o } LIMIT 10";
        
        // Execute the query and get results
        final TupleQueryResult result = repository.getConnection().prepareTupleQuery(queryString).evaluate();

        // Process and print each result binding
        while (result.hasNext()) {
            final BindingSet bindingSet = result.next();
            System.out.println(bindingSet);
        }
    }
}
