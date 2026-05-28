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

import com.amazonaws.neptune.auth.NeptuneJavaHttpSigV4Signer;
import com.amazonaws.neptune.auth.NeptuneSigV4SignerException;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import java.io.IOException;
import java.net.Authenticator;
import java.net.CookieHandler;
import java.net.ProxySelector;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * AWS Signature Version 4 signing HTTP client for Amazon Neptune SPARQL requests.
 * <p>
 * This class extends the standard Java HttpClient to automatically sign HTTP requests
 * using AWS Signature Version 4 authentication via {@link NeptuneJavaHttpSigV4Signer}.
 * It's specifically designed for use with Amazon Neptune database instances that require
 * IAM authentication.
 * <p>
 * The client wraps a delegate HttpClient and intercepts requests to add the necessary
 * AWS authentication headers before forwarding them to the underlying client.
 *
 * @author Charles Ivie
 */
public class AwsSigningHttpClient extends HttpClient {

    /** The underlying HTTP client that handles actual network communication */
    private final HttpClient delegate;

    /** The SigV4 signer for java.net.http requests */
    private final NeptuneJavaHttpSigV4Signer signer;

    /**
     * Creates a new AWS signing HTTP client.
     *
     * @param serviceName the AWS service name for signing (e.g., "neptune-db")
     * @param region the AWS region where the Neptune instance is located
     * @param credentialsProvider the AWS credentials provider for authentication
     * @param requestBody the request body content (unused, kept for API compatibility)
     */
    public AwsSigningHttpClient(
            String serviceName,
            Region region,
            AwsCredentialsProvider credentialsProvider,
            String requestBody) {
        try {
            this.signer = new NeptuneJavaHttpSigV4Signer(region.id(), credentialsProvider, serviceName);
        } catch (NeptuneSigV4SignerException e) {
            throw new IllegalArgumentException("Failed to initialize SigV4 signer", e);
        }
        this.delegate = HttpClient.newHttpClient();
    }

    @Override
    public <T> HttpResponse<T> send(HttpRequest request, HttpResponse.BodyHandler<T> responseBodyHandler)
            throws IOException, InterruptedException {
        return delegate.send(signRequest(request), responseBodyHandler);
    }

    @Override
    public <T> CompletableFuture<HttpResponse<T>> sendAsync(HttpRequest request, HttpResponse.BodyHandler<T> responseBodyHandler) {
        return delegate.sendAsync(signRequest(request), responseBodyHandler);
    }

    @Override
    public <T> CompletableFuture<HttpResponse<T>> sendAsync(HttpRequest r, HttpResponse.BodyHandler<T> h, HttpResponse.PushPromiseHandler<T> p) {
        return delegate.sendAsync(signRequest(r), h, p);
    }

    private HttpRequest signRequest(HttpRequest request) {
        HttpRequest.Builder builder = HttpRequest.newBuilder(request, (name, value) -> true);
        try {
            signer.signRequest(builder);
        } catch (NeptuneSigV4SignerException e) {
            throw new RuntimeException("Failed to sign request", e);
        }
        return builder.build();
    }

    @Override
    public Optional<CookieHandler> cookieHandler() {
        return delegate.cookieHandler();
    }

    @Override
    public Optional<Duration> connectTimeout() {
        return delegate.connectTimeout();
    }

    @Override
    public Redirect followRedirects() {
        return delegate.followRedirects();
    }

    @Override
    public Optional<ProxySelector> proxy() {
        return delegate.proxy();
    }

    @Override
    public SSLContext sslContext() {
        return delegate.sslContext();
    }

    @Override
    public SSLParameters sslParameters() {
        return delegate.sslParameters();
    }

    @Override
    public Optional<Authenticator> authenticator() {
        return delegate.authenticator();
    }

    @Override
    public Version version() {
        return delegate.version();
    }

    @Override
    public Optional<Executor> executor() {
        return delegate.executor();
    }
}
