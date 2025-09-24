package com.amazonaws.neptune.integration;

import com.amazonaws.neptune.client.rdf4j.NeptuneSparqlRepository;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.Update;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;

import java.util.UUID;

import static com.amazonaws.neptune.NeptuneConnectionIntegrationTestBase.getInsertQuery;
import static com.amazonaws.neptune.NeptuneConnectionIntegrationTestBase.getSelectQuery;
import static org.junit.jupiter.api.Assertions.*;

@EnabledIfSystemProperty(named = "neptune.endpoint", matches = ".*")
class NeptuneRdf4JIntegrationTest {

    private Repository repository;
    private String testGraphUri;

    @BeforeEach
    void setUp() throws Exception {

        String neptuneEndpoint = System.getProperty(
                "neptune.endpoint",
                "https://xxxx.xxxx.us-west-1.neptune.amazonaws.com:8182/");
        String regionName = System.getProperty("aws.region", "us-west-1");

        assertNotNull(neptuneEndpoint, "Neptune endpoint must be provided via -Dneptune.endpoint=<endpoint>");

        repository = new NeptuneSparqlRepository(
                neptuneEndpoint,
                DefaultCredentialsProvider.create(),
                regionName
        );
        repository.init();

        testGraphUri = "http://neptune.aws.com/ontology/testing/" + UUID.randomUUID();
    }

    @Test
    void testInsertAndQueryWithRdf4J() throws Exception {

        try (RepositoryConnection conn = repository.getConnection()) {
            // Insert test data
            Update update = conn.prepareUpdate(getInsertQuery(testGraphUri));
            update.execute();
            System.out.println("✓ RDF4J Insert completed successfully");

            // Query and verify data
            TupleQuery tupleQuery = conn.prepareTupleQuery(getSelectQuery(testGraphUri));

            long resultCount = tupleQuery.evaluate().stream().count();

            assertEquals(1, resultCount, "Should find exactly 1 result");
            System.out.println("✓ RDF4J Query completed successfully");

        } catch (Exception e) {
            fail("RDF4J Insert and Query test failed: " + e.getMessage());
        }
    }

    @Test
    void testRepositoryConnection() throws Exception {
        try (RepositoryConnection conn = repository.getConnection()) {
            assertTrue(conn.isOpen());
            assertNotNull(conn.getValueFactory());
            System.out.println("✓ RDF4J Repository connection test passed");
        }
    }

    @AfterEach
    void tearDown() {
        String deleteQuery = String.format("DROP GRAPH <%s>", testGraphUri);

        try (RepositoryConnection conn = repository.getConnection()) {
            Update update = conn.prepareUpdate(deleteQuery);
            update.execute();
            System.out.println("✓ RDF4J Cleanup completed");
        } catch (Exception e) {
            System.err.println("RDF4J Cleanup failed: " + e.getMessage());
        }
    }
}