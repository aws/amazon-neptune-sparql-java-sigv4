package com.amazonaws.neptune.client.jena;

import org.apache.jena.rdfconnection.RDFConnection;
import org.apache.jena.rdfconnection.RDFConnectionRemote;
import org.apache.jena.rdfconnection.RDFConnectionRemoteBuilder;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;

import java.net.http.HttpClient;

public class NeptuneJenaSigV4Example {

    public static void main(String... args) {

        final String endpoint = "https://playground.cluster-cfk6p1jkvase.us-west-1.neptune.amazonaws.com:8182";
        final String regionName = "us-west-1";
        final AwsCredentialsProvider awsCredentialsProvider = DefaultCredentialsProvider.builder().build();

        String query = "SELECT * { ?s ?p ?o } LIMIT 100";

        // Create signing HTTP client
        HttpClient signingClient = new AwsSigningHttpClient(
                "neptune-db",
                Region.of(regionName),
                awsCredentialsProvider,
                query
        );

        // Build Jena connection with signing client
        RDFConnectionRemoteBuilder builder = RDFConnectionRemote.create()
                .httpClient(signingClient)
                .destination(endpoint)
                .queryEndpoint("sparql")
                .updateEndpoint("sparql");


        try (RDFConnection conn = builder.build()) {
            System.out.println("> Printing query result: ");
            conn.querySelect(query, System.out::println);
        }

    }

}
