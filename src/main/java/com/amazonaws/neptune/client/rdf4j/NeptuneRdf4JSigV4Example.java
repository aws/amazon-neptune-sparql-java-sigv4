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

public class NeptuneRdf4JSigV4Example {

    public static void main(final String[] args) throws NeptuneSigV4SignerException {

        // Create a NeptuneSparqlRepository with V4 signing enabled
        final String endpointUrl = "https://playground.cluster-cfk6p1jkvase.us-west-1.neptune.amazonaws.com:8182/";
        final String regionName = "us-west-1";
        final AwsCredentialsProvider awsCredentialsProvider = DefaultCredentialsProvider.create();

        final NeptuneSparqlRepository repository = new NeptuneSparqlRepository(
                endpointUrl,
                awsCredentialsProvider,
                regionName
        );

        // Query the repository
        final String queryString = "SELECT ?s ?p ?o WHERE { ?s ?p ?o } LIMIT 10";
        final TupleQueryResult result = repository.getConnection().prepareTupleQuery(queryString).evaluate();

        // Print the results
        while (result.hasNext()) {
            final BindingSet bindingSet = result.next();
            System.out.println(bindingSet);
        }
    }
}
