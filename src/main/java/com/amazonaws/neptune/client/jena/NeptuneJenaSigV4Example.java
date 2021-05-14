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

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.neptune.auth.NeptuneApacheHttpSigV4Signer;
import com.amazonaws.neptune.auth.NeptuneSigV4SignerException;
import com.amazonaws.util.StringUtils;
import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.protocol.HttpContext;
import org.apache.jena.rdfconnection.RDFConnection;
import org.apache.jena.rdfconnection.RDFConnectionRemote;
import org.apache.jena.rdfconnection.RDFConnectionRemoteBuilder;

/*
 * Example of a building a remote connection
 */
public class NeptuneJenaSigV4Example {

    public static void main(String... args) throws NeptuneSigV4SignerException {

        if (args.length == 0 || StringUtils.isNullOrEmpty(args[0])) {
            System.err.println("Please specify your endpoint as program argument "
                    + "(e.g.: http://<your_neptune_endpoint>:<your_neptune_endpoint>)");
            System.exit(1);
        }

        final String endpoint = args[0];

        final String regionName = "us-east-1";
        final AWSCredentialsProvider awsCredentialsProvider = new DefaultAWSCredentialsProviderChain();

        final NeptuneApacheHttpSigV4Signer v4Signer = new NeptuneApacheHttpSigV4Signer(regionName, awsCredentialsProvider);

        final HttpClient v4SigningClient = HttpClientBuilder.create().addInterceptorLast(new HttpRequestInterceptor() {

            @Override
            public void process(final HttpRequest req, final HttpContext ctx) throws HttpException {

                if (req instanceof HttpUriRequest) {

                    final HttpUriRequest httpUriReq = (HttpUriRequest) req;
                    try {
                        v4Signer.signRequest(httpUriReq);
                    } catch (NeptuneSigV4SignerException e) {
                        throw new HttpException("Problem signing the request: ", e);
                    }

                } else {

                    throw new HttpException("Not an HttpUriRequest"); // this should never happen
                }
            }

        }).build();

        RDFConnectionRemoteBuilder builder = RDFConnectionRemote.create()
                .httpClient(v4SigningClient)
                .destination(endpoint)
                // Query only.
                .queryEndpoint("sparql")
                .updateEndpoint("sparql");

        String query = "SELECT * { ?s ?p ?o } LIMIT 100";

        // Whether the connection can be reused depends on the details of the implementation.
        // See example 5.
        try (RDFConnection conn = builder.build()) {
            System.out.println("> Printing query result: ");
            conn.querySelect(query, System.out::println);
        }
    }
}

