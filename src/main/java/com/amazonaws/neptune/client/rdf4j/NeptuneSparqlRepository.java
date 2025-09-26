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

import com.amazonaws.neptune.auth.NeptuneApacheHttpSigV4Signer;
import com.amazonaws.neptune.auth.NeptuneSigV4Signer;
import com.amazonaws.neptune.auth.NeptuneSigV4SignerException;
import org.apache.http.HttpException;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.HttpClientBuilder;
import org.eclipse.rdf4j.repository.sparql.SPARQLRepository;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;


/**
 * SPARQL repository implementation for connecting to Amazon Neptune instances.
 * <p>
 * This repository extends Eclipse RDF4J's SPARQLRepository to provide seamless integration
 * with Amazon Neptune databases. It supports both authenticated and unauthenticated connections:
 * <ul>
 *   <li><strong>Authenticated connections:</strong> Use AWS Signature Version 4 authentication
 *       for secure access to Neptune instances with IAM-based access control</li>
 *   <li><strong>Unauthenticated connections:</strong> Direct connections for Neptune instances
 *       without IAM authentication enabled</li>
 * </ul>
 * <p>
 * For authenticated connections, the repository automatically signs HTTP requests using
 * AWS Signature V4 as described in the 
 * <a href="https://docs.aws.amazon.com/general/latest/gr/signature-version-4.html">AWS documentation</a>.
 * <p>
 * <strong>Usage Examples:</strong>
 * <pre>{@code
 * // Unauthenticated connection
 * NeptuneSparqlRepository repo = new NeptuneSparqlRepository("https://my-neptune-cluster:8182");
 * 
 * // Authenticated connection
 * NeptuneSparqlRepository repo = new NeptuneSparqlRepository(
 *     "https://my-neptune-cluster:8182",
 *     DefaultCredentialsProvider.create(),
 *     "us-east-1"
 * );
 * }</pre>
 *
 * @author AWS Neptune Team
 * @since 1.0
 */
public class NeptuneSparqlRepository extends SPARQLRepository {

    /**
     * The name of the region in which Neptune is running.
     */
    private final String regionName;

    /**
     * Whether authentication is enabled.
     */
    private final boolean authenticationEnabled;

    /**
     * The credentials provider, offering credentials for signing the request.
     */
    private final AwsCredentialsProvider awsCredentialsProvider;

    /**
     * The signature V4 signer used to sign the request.
     */
    private NeptuneSigV4Signer<HttpUriRequest> v4Signer;

    /**
     * Creates a Neptune SPARQL repository without authentication.
     * <p>
     * Use this constructor for Neptune instances that don't require IAM authentication.
     * The repository will connect directly to the Neptune SPARQL endpoint without
     * adding any AWS signature headers.
     *
     * @param endpointUrl the fully qualified Neptune cluster endpoint URL
     *                   (e.g., "https://my-cluster.cluster-xyz.us-east-1.neptune.amazonaws.com:8182/sparql")
     */
    public NeptuneSparqlRepository(final String endpointUrl) {
        super(endpointUrl);

        // all the fields below are only relevant for authentication and can be ignored
        this.authenticationEnabled = false;
        this.awsCredentialsProvider = null; // only needed if auth is enabled
        this.regionName = null; // only needed if auth is enabled
    }

    /**
     * Creates a Neptune SPARQL repository with AWS Signature V4 authentication.
     * <p>
     * Use this constructor for Neptune instances that require IAM authentication.
     * The repository will automatically sign all HTTP requests using AWS Signature V4
     * before sending them to Neptune.
     *
     * @param endpointUrl fully qualified Neptune cluster endpoint URL
     *                   (e.g., "https://my-cluster.cluster-xyz.us-east-1.neptune.amazonaws.com:8182/sparql")
     * @param awsCredentialsProvider the AWS credentials provider for obtaining signing credentials
     *                              (e.g., DefaultCredentialsProvider.create())
     * @param regionName the AWS region name where the Neptune cluster is located (e.g., "us-east-1")
     * @throws NeptuneSigV4SignerException if there's an error initializing the AWS signature signer
     */
    public NeptuneSparqlRepository(
            final String endpointUrl,
            final AwsCredentialsProvider awsCredentialsProvider,
            final String regionName)
            throws NeptuneSigV4SignerException {

        super(endpointUrl);

        this.authenticationEnabled = true;
        this.awsCredentialsProvider = awsCredentialsProvider;
        this.regionName = regionName;

        initAuthenticatingHttpClient();

    }

    /**
     * Initializes the HTTP client with AWS Signature V4 signing capability.
     * <p>
     * This method configures an Apache HTTP client with a request interceptor that
     * automatically signs outgoing requests using AWS Signature V4. The interceptor
     * is added last in the chain to ensure it operates on the final request.
     *
     * @throws NeptuneSigV4SignerException if there's an error initializing the AWS signature signer
     */
    protected void initAuthenticatingHttpClient() throws NeptuneSigV4SignerException {

        if (!authenticationEnabled) {
            return; // Authentication not enabled, skip HTTP client configuration
        }

        // Initialize AWS Signature V4 signer for Apache HTTP requests
        v4Signer = new NeptuneApacheHttpSigV4Signer(regionName, awsCredentialsProvider);

        /*
         * Configure HTTP client with signing interceptor.
         * The interceptor is added last to ensure it operates on the final version
         * of the request after all other interceptors have processed it.
         */
        final HttpClient v4SigningClient = HttpClientBuilder
                .create()
                .addInterceptorLast((HttpRequestInterceptor) (req, ctx) -> {
                    // Ensure the request is the expected type
                    if (req instanceof HttpUriRequest httpUriReq) {
                        try {
                            // Sign the request using AWS Signature V4
                            v4Signer.signRequest(httpUriReq);
                        } catch (NeptuneSigV4SignerException e) {
                            throw new HttpException("Failed to sign Neptune request with AWS Signature V4: ", e);
                        }
                    } else {
                        // This should never happen with standard HTTP clients
                        throw new HttpException("Request is not an HttpUriRequest instance");
                    }
                }).build();

        setHttpClient(v4SigningClient);

    }

}

