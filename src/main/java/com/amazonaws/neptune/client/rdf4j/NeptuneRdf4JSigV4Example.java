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

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.neptune.auth.NeptuneSigV4SignerException;
import com.amazonaws.util.StringUtils;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.query.Update;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;

/**
 * Small example demonstrating how to use NeptuneSparqlRepository with SignatureV4 in combination
 * with the RDF4J library (see http://rdf4j.org/). The example uses the {@link NeptuneSparqlRepository}
 * class contained in this package, which extends RDF4J's SparqlRepository class by IAM authentication.
 * <p>
 * Before running this code, make sure you've got everything setup properly, in particular:
 * <ol>
 *     <li> Make sure that your AWS credentials are available in the provider chain, see
 *          <a href="https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/DefaultAWSCredentialsProviderChain.html">
 *              DefaultAWSCredentialsProviderChain</a> for more information.</li>
 *     <li> Start the main method by passing in your endpoint, e.g. "http://&lt;my_neptune_host&gt;:&lt;my_neptune_port&gt;".
 *          The server will send a <code>"SELECT * WHERE { ?s ?p ?o } LIMIT 10"</code> query against your endpoint.</li>
 * </ol>
 *
 * @author schmdtm
 */
public final class NeptuneRdf4JSigV4Example {

    /**
     * Region in which the Neptune instance runs.
     */
    private static final String TEST_REGION = "us-east-1";

    /**
     * Sample select query, limited to ten results.
     */
    private static final String SAMPLE_QUERY = "SELECT * WHERE { ?s ?p ?o } LIMIT 10";

    /**
     * Sample SPARQL UPDATE query.
     */
    private static final String SAMPLE_UPDATE = "INSERT DATA { <http://Alice> <http://knows> <http://Bob> }";

    /**
     * Expected exception when sending an unsigned request to an auth enabled neptune server.
     */
    static final String ACCESS_DENIED_MSG = "{\"status\":\"403 Forbidden\",\"message\":\"Access Denied!\"}";


    /**
     * Main method. Expecting the endpoint as the first argument.
     *
     * @param args arguments
     * @throws Exception in case there are problems
     */
    public static void main(final String[] args) throws Exception {

        if (args.length == 0 || StringUtils.isNullOrEmpty(args[0])) {
            System.err.println("Please specify your endpoint as program argument "
                    + "(e.g.: http://<my_neptune_host>:<my_neptune_port>)");
            System.exit(1);
        }
        final String endpoint = args[0];

        // example of sending a signed query against the SPARQL endpoint
        // use default SAMPLE_QUERY if not specified from input args
        final String query = (args.length > 1 && !StringUtils.isNullOrEmpty(args[1])) ? args[1] : SAMPLE_QUERY;
        executeSignedQueryRequest(endpoint, query);
    }

    /**
     * Example for signed request.
     *
     * @param endpointUrl of the endpoint to which to send the request
     * @throws NeptuneSigV4SignerException in case there's a problem signing the request
     */
    protected static void executeSignedQueryRequest(final String endpointUrl, final String query)
            throws NeptuneSigV4SignerException {

        final AWSCredentialsProvider awsCredentialsProvider = new DefaultAWSCredentialsProviderChain();
        final NeptuneSparqlRepository neptuneSparqlRepo =
                new NeptuneSparqlRepository(endpointUrl, awsCredentialsProvider, TEST_REGION);

        try {

            neptuneSparqlRepo.initialize();
            evaluateAndPrintQueryResult(query, neptuneSparqlRepo);

        } finally {

            neptuneSparqlRepo.shutDown();

        }
    }

    /**
     * Example for signed request.
     *
     * @param endpointUrl of the endpoint to which to send the request
     * @throws NeptuneSigV4SignerException in case there's a problem signing the request
     */
    protected static void executeSignedInsertRequest(final String endpointUrl)
            throws NeptuneSigV4SignerException {

        final AWSCredentialsProvider awsCredentialsProvider = new DefaultAWSCredentialsProviderChain();

        final NeptuneSparqlRepository neptuneSparqlRepo =
                new NeptuneSparqlRepository(endpointUrl, awsCredentialsProvider, TEST_REGION);

        try {
            neptuneSparqlRepo.initialize();

            try (RepositoryConnection conn = neptuneSparqlRepo.getConnection()) {

                final Update update = conn.prepareUpdate(SAMPLE_UPDATE);
                update.execute();
                System.out.println("Update query executed!");

            }

        } finally {

            neptuneSparqlRepo.shutDown();
        }
    }


    /**
     * Example for unsigned request.
     *
     * @param endpointUrl of the endpoint to which to send the request
     */
    protected static void executeUnsignedQueryRequest(final String endpointUrl) {

        // use the simple constructor version which skips auth initialization
        final NeptuneSparqlRepository neptuneSparqlRepo = new NeptuneSparqlRepository(endpointUrl);

        try {

            neptuneSparqlRepo.initialize();
            evaluateAndPrintQueryResult(SAMPLE_QUERY, neptuneSparqlRepo);

        } finally {

            neptuneSparqlRepo.shutDown();

        }
    }


    /**
     * Evaluate the query and print the query result.
     *
     * @param queryString the query string to evaluate
     * @param repo        the repository over which to evaluate the query
     */
    protected static void evaluateAndPrintQueryResult(final String queryString, final Repository repo) {

        try (RepositoryConnection conn = repo.getConnection()) {

            final TupleQuery query = conn.prepareTupleQuery(queryString);

            System.out.println("> Printing query result: ");
            final TupleQueryResult res = query.evaluate();

            while (res.hasNext()) {
                System.err.println("{");
                final BindingSet bs = res.next();
                boolean first = true;
                for (final String varName : bs.getBindingNames()) {
                    if (first) {
                        System.out.print("  { ");
                    } else {
                        System.out.print(", ");
                    }
                    System.out.print("?" + varName + " -> " + bs.getBinding(varName));

                    first = false;
                }
                System.out.println("}");
                System.out.println("}");
            }
        }
    }

    /**
     * Constructor.
     */
    private NeptuneRdf4JSigV4Example() {
    }

}
