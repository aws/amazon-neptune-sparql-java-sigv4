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

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.signer.Aws4Signer;
import software.amazon.awssdk.auth.signer.params.Aws4SignerParams;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.regions.Region;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.Authenticator;
import java.net.CookieHandler;
import java.net.ProxySelector;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * AWS Signature Version 4 signing HTTP client for Amazon Neptune SPARQL requests.
 * <p>
 * This class extends the standard Java HttpClient to automatically sign HTTP requests
 * using AWS Signature Version 4 authentication. It's specifically designed for use with
 * Amazon Neptune database instances that require IAM authentication.
 * <p>
 * The client wraps a delegate HttpClient and intercepts requests to add the necessary
 * AWS authentication headers (Authorization and x-amz-date) before forwarding them
 * to the underlying client.
 * 
 * @author AWS Neptune Team
 * @since 1.0
 */
public class AwsSigningHttpClient extends HttpClient {

    /** The underlying HTTP client that handles actual network communication */
    private final HttpClient delegate;
    
    /** The request body content used for signature calculation */
    private final String requestBody;
    
    /** AWS credentials provider for obtaining signing credentials */
    private final AwsCredentialsProvider credentialsProvider;
    
    /** AWS service name for signing (typically "neptune-db") */
    private final String serviceName;
    
    /** AWS region where the Neptune instance is located */
    private final Region region;

    /**
     * Creates a new AWS signing HTTP client.
     * 
     * @param serviceName the AWS service name for signing (e.g., "neptune-db")
     * @param region the AWS region where the Neptune instance is located
     * @param credentialsProvider the AWS credentials provider for authentication
     * @param requestBody the request body content used for signature calculation
     */
    public AwsSigningHttpClient(
            String serviceName,
            Region region,
            AwsCredentialsProvider credentialsProvider,
            String requestBody) {
        this.requestBody = requestBody;
        this.credentialsProvider = credentialsProvider;
        this.serviceName = serviceName;
        this.region = region;
        // The delegate client will handle the actual network communication.
        this.delegate = HttpClient.newHttpClient();
    }

    /**
     * Sends a synchronous HTTP request with AWS Signature V4 authentication.
     * 
     * @param <T> the response body type
     * @param request the HTTP request to send
     * @param responseBodyHandler the response body handler
     * @return the HTTP response
     * @throws IOException if an I/O error occurs
     * @throws InterruptedException if the operation is interrupted
     */
    @Override
    public <T> HttpResponse<T> send(HttpRequest request, HttpResponse.BodyHandler<T> responseBodyHandler)
            throws IOException, InterruptedException {
        return delegate.send(buildSignedRequest(request), responseBodyHandler);
    }

    /**
     * Sends an asynchronous HTTP request with AWS Signature V4 authentication.
     * 
     * @param <T> the response body type
     * @param request the HTTP request to send
     * @param responseBodyHandler the response body handler
     * @return a CompletableFuture containing the HTTP response
     */
    @Override
    public <T> CompletableFuture<HttpResponse<T>> sendAsync(HttpRequest request, HttpResponse.BodyHandler<T> responseBodyHandler) {
        return delegate.sendAsync(buildSignedRequest(request), responseBodyHandler);
    }

    /**
     * Builds a signed HTTP request by adding AWS Signature V4 authentication headers.
     * 
     * @param request the original HTTP request
     * @return a new HTTP request with AWS authentication headers added
     */
    private HttpRequest buildSignedRequest(HttpRequest request) {
        // Create a builder from the original request, copying all headers
        HttpRequest.Builder signedRequestBuilder = HttpRequest.newBuilder(request, (name, value) -> true);

        // Convert the Java HTTP request to AWS SDK format for signing
        SdkHttpFullRequest awsRequestForSigning = toSdkRequest(request, requestBody);

        // Configure the AWS Signature V4 signer parameters
        Aws4SignerParams signerParams = Aws4SignerParams.builder()
                .awsCredentials(credentialsProvider.resolveCredentials())
                .signingName(serviceName)
                .signingRegion(region)
                .build();

        // Sign the request using AWS Signature V4
        SdkHttpFullRequest signedSdkRequest = Aws4Signer.create().sign(awsRequestForSigning, signerParams);

        // Add the authentication headers to the original request
        return signedRequestBuilder
                .header("Authorization", signedSdkRequest.headers().get("Authorization").get(0))
                .header("x-amz-date", signedSdkRequest.headers().get("x-amz-date").get(0))
                .build();
    }


    // Delegate all other abstract methods to the wrapped client
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

    /**
     * Sends an asynchronous HTTP request with push promise support and AWS Signature V4 authentication.
     * 
     * @param <T> the response body type
     * @param r the HTTP request to send
     * @param h the response body handler
     * @param p the push promise handler
     * @return a CompletableFuture containing the HTTP response
     */
    @Override
    public <T> CompletableFuture<HttpResponse<T>> sendAsync(HttpRequest r, HttpResponse.BodyHandler<T> h, HttpResponse.PushPromiseHandler<T> p) {
        return delegate.sendAsync(buildSignedRequest(r), h, p);
    }

    /**
     * Converts a Java HttpRequest to an AWS SDK SdkHttpFullRequest for signing.
     * 
     * @param originalHttpRequest the original Java HTTP request
     * @param originalBody the request body content
     * @return an AWS SDK HTTP request ready for signing
     */
    private SdkHttpFullRequest toSdkRequest(HttpRequest originalHttpRequest, String originalBody) {
        // Build the AWS SDK request with URI and HTTP method
        SdkHttpFullRequest.Builder sdkRequestBuilder = SdkHttpFullRequest.builder()
                .uri(originalHttpRequest.uri())
                .method(SdkHttpMethod.fromValue(originalHttpRequest.method()));

        // Add request body if present
        if (originalHttpRequest.bodyPublisher().isPresent()) {
            sdkRequestBuilder.contentStreamProvider(() -> new ByteArrayInputStream(originalBody.getBytes(StandardCharsets.UTF_8)));
        }

        // Copy all headers from the original request
        originalHttpRequest.headers().map().forEach((headerName, headerValues) -> {
            for (String headerValue : headerValues) {
                sdkRequestBuilder.putHeader(headerName, headerValue);
            }
        });

        return sdkRequestBuilder.build();
    }
}
