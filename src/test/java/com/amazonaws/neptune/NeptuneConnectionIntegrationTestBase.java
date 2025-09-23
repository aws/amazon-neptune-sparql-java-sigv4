package com.amazonaws.neptune;

public class NeptuneConnectionIntegrationTestBase {

    public static String getInsertQuery(String namedGraph) {
        return String.format(
                "INSERT DATA {" +
                        "   GRAPH <%s> {" +
                        "       <urn:test> <urn:test> <urn:test> " +
                        "   }" +
                        "}", namedGraph);
    }

    public static String getSelectQuery(String namedGraph) {
        return String.format(
                "SELECT * {" +
                        "   GRAPH <%s> {" +
                        "       ?s ?p ?o " +
                        "   }" +
                        "}", namedGraph);
    }

    public static String getClearQuery(String namedGraph) {
        return String.format(
                "CLEAR GRAPH <%s>", namedGraph);
    }
}
