package com.amazonaws.neptune.client.jena;

import com.amazonaws.neptune.auth.NeptuneSigV4SignerException;
import software.amazon.awssdk.utils.StringUtils;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import org.apache.jena.rdfconnection.RDFConnection;
import org.apache.jena.rdfconnection.RDFConnectionRemote;
import org.apache.jena.rdfconnection.RDFConnectionRemoteBuilder;

import java.net.http.HttpClient;

public class NeptuneJenaSigV4Example {

    public static void main(String... args) throws NeptuneSigV4SignerException {

        if (args.length == 0 || StringUtils.isEmpty(args[0])) {
            System.err.println("Please specify your endpoint as program argument "
                    + "(e.g.: http://<your_neptune_endpoint>:<your_neptune_endpoint>)");
            System.exit(1);
        }

        final String endpoint = args[0];
        final String regionName = "us-east-1";
        final AwsCredentialsProvider awsCredentialsProvider = DefaultCredentialsProvider.builder().build();

        // Create signing HTTP client
        HttpClient signingClient = SigningHttpClient.forNeptune(awsCredentialsProvider, regionName);

        // Build Jena connection with signing client
        RDFConnectionRemoteBuilder builder = RDFConnectionRemote.create()
                .httpClient(signingClient)
                .destination(endpoint)
                .queryEndpoint("sparql")
                .updateEndpoint("sparql");

        String query = "SELECT * { ?s ?p ?o } LIMIT 100";

        try (RDFConnection conn = builder.build()) {
            System.out.println("> Printing query result: ");
            conn.querySelect(query, System.out::println);
        }
    }
}
