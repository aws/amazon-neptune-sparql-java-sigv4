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

package com.amazonaws.neptune;

/**
 * Base class providing utility methods for Neptune integration tests.
 * <p>
 * This class contains helper methods for generating common SPARQL queries
 * used in integration testing with Amazon Neptune. It provides standardized
 * query templates for inserting, selecting, and clearing test data.
 * <p>
 * The queries use named graphs to isolate test data and avoid conflicts
 * between different test runs or test methods.
 * 
 * @author AWS Neptune Team
 * @since 1.0
 */
public class NeptuneConnectionIntegrationTestBase {

    /**
     * Generates a SPARQL INSERT query for adding test data to a named graph.
     * <p>
     * Creates a simple triple with test URNs that can be used to verify
     * that data insertion operations are working correctly.
     * 
     * @param namedGraph the URI of the named graph where data should be inserted
     * @return a SPARQL INSERT DATA query string
     */
    public static String getInsertQuery(String namedGraph) {
        return String.format(
                "INSERT DATA {" +
                        "   GRAPH <%s> {" +
                        "       <urn:test> <urn:test> <urn:test> " +  // Simple test triple
                        "   }" +
                        "}", namedGraph);
    }

    /**
     * Generates a SPARQL SELECT query for retrieving all triples from a named graph.
     * <p>
     * Returns all subject-predicate-object triples stored in the specified
     * named graph, useful for verifying that data was inserted correctly.
     * 
     * @param namedGraph the URI of the named graph to query
     * @return a SPARQL SELECT query string that retrieves all triples from the graph
     */
    public static String getSelectQuery(String namedGraph) {
        return String.format(
                "SELECT * {" +
                        "   GRAPH <%s> {" +
                        "       ?s ?p ?o " +  // Select all triples in the graph
                        "   }" +
                        "}", namedGraph);
    }

    /**
     * Generates a SPARQL CLEAR query for removing all data from a named graph.
     * <p>
     * Removes all triples from the specified named graph, useful for cleaning
     * up test data after test execution to ensure test isolation.
     * 
     * @param namedGraph the URI of the named graph to clear
     * @return a SPARQL CLEAR GRAPH query string
     */
    public static String getClearQuery(String namedGraph) {
        return String.format(
                "CLEAR GRAPH <%s>", namedGraph);  // Remove all triples from the graph
    }
}
