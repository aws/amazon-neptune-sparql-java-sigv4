## Amazon Neptune Sparql Java Sigv4

A SPARQL client for [Amazon Neptune](https://aws.amazon.com/neptune) that includes AWS Signature Version 4 signing. Implemented as an RDF4J repository and Jena HTTP Client. This library relies on [amazon-neptune-sigv4-signer](https://github.com/aws/amazon-neptune-sigv4-signer/).

## ⚠️ SDK v1 Deprecation Notice

As of version 4.1.0, the underlying [amazon-neptune-sigv4-signer](https://github.com/aws/amazon-neptune-sigv4-signer/) no longer transitively pulls in `aws-java-sdk-core` (AWS SDK v1). SDK v1 support is deprecated and will be removed in a future major version.

If you use SDK v1 credentials (`com.amazonaws.auth.AWSCredentialsProvider`), you can still do so by explicitly adding `aws-java-sdk-core` as a dependency in your project. However, we recommend migrating to SDK v2 credentials (`software.amazon.awssdk.auth.credentials.AwsCredentialsProvider`) — for example, `DefaultCredentialsProvider.create()`.

For example usage refer to:
 
-	[NeptuneJenaSigV4Example.java](https://github.com/aws/amazon-neptune-sparql-java-sigv4/blob/master/src/main/java/com/amazonaws/neptune/client/jena/NeptuneJenaSigV4Example.java) 
-	[NeptuneRdf4JSigV4Example.java](https://github.com/aws/amazon-neptune-sparql-java-sigv4/blob/master/src/main/java/com/amazonaws/neptune/client/rdf4j/NeptuneRdf4JSigV4Example.java) 

For the official Amazon Neptune page refer to: https://aws.amazon.com/neptune

## License

This library is licensed under the Apache 2.0 License. 
