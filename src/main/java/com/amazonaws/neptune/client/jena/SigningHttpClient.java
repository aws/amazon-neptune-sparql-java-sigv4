package com.amazonaws.neptune.client.jena;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain;
import software.amazon.awssdk.http.auth.aws.signer.AwsV4HttpSigner;
import software.amazon.awssdk.http.auth.spi.signer.SignerProperty;
import software.amazon.awssdk.http.auth.spi.signer.SignRequest;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.identity.spi.AwsCredentialsIdentity;

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

public class SigningHttpClient extends HttpClient {

    private final HttpClient delegate;
    private final AwsCredentialsProvider credentialsProvider;
    private final String serviceName;
    private final Region region;
    private final AwsV4HttpSigner signer;

    // Constructor with explicit parameters (highest precedence)
    public SigningHttpClient(AwsCredentialsProvider credentialsProvider, String serviceName, String regionName) {
        this(credentialsProvider, serviceName, Region.of(regionName));
    }

    public SigningHttpClient(AwsCredentialsProvider credentialsProvider, String serviceName, Region region) {
        this.delegate = HttpClient.newHttpClient();
        this.credentialsProvider = credentialsProvider;
        this.serviceName = serviceName;
        this.region = region;
        this.signer = AwsV4HttpSigner.create();
    }

    // Constructor using AWS provider chains and environment variables
    public SigningHttpClient(AwsCredentialsProvider credentialsProvider) {
        this.delegate = HttpClient.newHttpClient();
        this.credentialsProvider = credentialsProvider;
        this.serviceName = resolveServiceName();
        this.region = resolveRegion();
        this.signer = AwsV4HttpSigner.create();
    }

    private String resolveServiceName() {
        // Precedence: System property -> Environment variable -> Default
        return Optional.ofNullable(System.getProperty("aws.neptune.serviceName"))
                .or(() -> Optional.ofNullable(System.getenv("AWS_NEPTUNE_SERVICE_NAME")))
                .orElse("neptune-db");
    }

    private Region resolveRegion() {
        // Use AWS SDK's default region provider chain
        return DefaultAwsRegionProviderChain.builder().build().getRegion();
    }

    // Factory methods for common scenarios
    public static SigningHttpClient forNeptune(AwsCredentialsProvider credentialsProvider, String regionName) {
        return new SigningHttpClient(credentialsProvider, "neptune-db", regionName);
    }

    public static SigningHttpClient forNeptuneAnalytics(AwsCredentialsProvider credentialsProvider, String regionName) {
        return new SigningHttpClient(credentialsProvider, "neptune-graph", regionName);
    }

    public static SigningHttpClient withDefaults(AwsCredentialsProvider credentialsProvider) {
        return new SigningHttpClient(credentialsProvider);
    }

    @Override
    public <T> HttpResponse<T> send(HttpRequest request, HttpResponse.BodyHandler<T> responseBodyHandler)
            throws IOException, InterruptedException {
        return delegate.send(signRequest(request), responseBodyHandler);
    }

    @Override
    public <T> CompletableFuture<HttpResponse<T>> sendAsync(HttpRequest request,
                                                            HttpResponse.BodyHandler<T> responseBodyHandler) {
        try {
            return delegate.sendAsync(signRequest(request), responseBodyHandler);
        } catch (Exception e) {
            CompletableFuture<HttpResponse<T>> future = new CompletableFuture<>();
            future.completeExceptionally(e);
            return future;
        }
    }

    @Override
    public <T> CompletableFuture<HttpResponse<T>> sendAsync(HttpRequest request,
                                                            HttpResponse.BodyHandler<T> responseBodyHandler,
                                                            HttpResponse.PushPromiseHandler<T> pushPromiseHandler) {
        try {
            return delegate.sendAsync(signRequest(request), responseBodyHandler, pushPromiseHandler);
        } catch (Exception e) {
            CompletableFuture<HttpResponse<T>> future = new CompletableFuture<>();
            future.completeExceptionally(e);
            return future;
        }
    }

    private HttpRequest signRequest(HttpRequest request) {
        try {
            SdkHttpFullRequest.Builder sdkRequestBuilder = SdkHttpFullRequest.builder()
                    .method(SdkHttpMethod.fromValue(request.method()))
                    .uri(request.uri());

            request.headers().map().forEach((name, values) ->
                    values.forEach(value -> sdkRequestBuilder.appendHeader(name, value)));

            SdkHttpFullRequest sdkRequest = sdkRequestBuilder.build();

            // Get the credentials identity
            AwsCredentialsIdentity identity = credentialsProvider.resolveCredentials();

            // Create signer properties
            SignerProperty<String> serviceSigningName = SignerProperty.create(AwsV4HttpSigner.class, "SERVICE_SIGNING_NAME");
            SignerProperty<Region> signingRegion = SignerProperty.create(AwsV4HttpSigner.class, "SIGNING_REGION");

            SignRequest<AwsCredentialsIdentity> signRequest = SignRequest.builder(identity)
                    .request(sdkRequest)
                    .putProperty(serviceSigningName, serviceName)
                    .putProperty(signingRegion, region)
                    .build();

            SdkHttpRequest signedRequest = signer.sign(signRequest).request();

            HttpRequest.Builder signedRequestBuilder = HttpRequest.newBuilder()
                    .uri(request.uri())
                    .method(request.method(), request.bodyPublisher().orElse(HttpRequest.BodyPublishers.noBody()));

            signedRequest.headers().forEach((name, values) ->
                    values.forEach(value -> signedRequestBuilder.header(name, value)));

            request.timeout().ifPresent(signedRequestBuilder::timeout);

            return signedRequestBuilder.build();

        } catch (Exception e) {
            throw new NeptuneJenaConnectionException("Failed to sign request", e);
        }
    }

    // Delegate methods
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
