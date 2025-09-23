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
 * A custom HttpClient that extends the base class to automatically sign
 * requests with AWS V4 signatures before sending them.
 */
public class AwsSigningHttpClient extends HttpClient {

    private final HttpClient delegate;
    private final String requestBody;
    private final AwsCredentialsProvider credentialsProvider;
    private final String serviceName;
    private final Region region;

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

    @Override
    public <T> HttpResponse<T> send(HttpRequest request, HttpResponse.BodyHandler<T> responseBodyHandler)
            throws IOException, InterruptedException {
        return delegate.send(buildSignedRequest(request), responseBodyHandler);
    }

    @Override
    public <T> CompletableFuture<HttpResponse<T>> sendAsync(HttpRequest request, HttpResponse.BodyHandler<T> responseBodyHandler) {
        return delegate.sendAsync(buildSignedRequest(request), responseBodyHandler);
    }

    private HttpRequest buildSignedRequest(HttpRequest request) {

        HttpRequest.Builder signedRequestBuilder = HttpRequest.newBuilder(request, (name, value) -> true);

        SdkHttpFullRequest awsRequestForSigning = toSdkRequest(request, requestBody);

        Aws4SignerParams signerParams = Aws4SignerParams.builder()
                .awsCredentials(credentialsProvider.resolveCredentials())
                .signingName(serviceName)
                .signingRegion(region)
                .build();

        SdkHttpFullRequest signedSdkRequest = Aws4Signer.create().sign(awsRequestForSigning, signerParams);

        return signedRequestBuilder
                .header("Authorization", signedSdkRequest.headers().get("Authorization").get(0))
                .header("x-amz-date", signedSdkRequest.headers().get("x-amz-date").get(0))
                .build();

    }


    // Delegate all other abstract methods to the wrapped client.
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

    @Override
    public <T> CompletableFuture<HttpResponse<T>> sendAsync(HttpRequest r, HttpResponse.BodyHandler<T> h, HttpResponse.PushPromiseHandler<T> p) {
        return delegate.sendAsync(buildSignedRequest(r), h, p);
    }

    private SdkHttpFullRequest toSdkRequest(HttpRequest originalHttpRequest, String originalBody) {

        SdkHttpFullRequest.Builder sdkRequestBuilder = SdkHttpFullRequest.builder()
                .uri(originalHttpRequest.uri())
                .method(SdkHttpMethod.fromValue(originalHttpRequest.method()));

        if (originalHttpRequest.bodyPublisher().isPresent()) {
            sdkRequestBuilder.contentStreamProvider(() -> new ByteArrayInputStream(originalBody.getBytes(StandardCharsets.UTF_8)));
        }

        originalHttpRequest.headers().map().forEach((headerName, headerValues) -> {
            for (String headerValue : headerValues) {
                sdkRequestBuilder.putHeader(headerName, headerValue);
            }
        });

        return sdkRequestBuilder.build();
    }
}
