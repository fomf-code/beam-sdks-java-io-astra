package org.apache.beam.sdk.io.astra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.dtsx.astra.sdk.db.DatabaseClient;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.astra.model.SimpleDataEntity;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.File;
import java.io.Serializable;
import java.util.UUID;

/**
 * Testing for AstraIO using CQL.
 */
@RunWith(JUnit4.class)
public class AstraIOTest extends AbstractAstraDbTest implements Serializable  {

    /**
     * Pipeline reference
     */
    @Rule
    public transient TestPipeline pipelineWrite = TestPipeline.create();

    /**
     * Pipeline reference
     */
    @Rule
    public transient TestPipeline pipelineRead = TestPipeline.create();

    /**
     * Create DB and table before running the test
     */
    @BeforeClass
    public static void beforeClass() {
        // GetSession will create 'beam_test_integrations' db and download SCB in '/tmp'
        Session session = getSession();
        // Create table for the Demo
        session.execute(SimpleDataEntity.cqlCreateTable());
        // Empty the table
        session.execute(SimpleDataEntity.cqlTruncateTable());
    }

    /**
     * Populate a DB
     */
    @Test
    public void should_write_1000_into_astra() {
        long top = System.currentTimeMillis();
        int recordCount = 10000;
        pipelineWrite
                .apply(Create.of(SimpleDataEntity.generateTestData(recordCount)))
                .apply(AstraIO.<SimpleDataEntity>write()
                        .withToken(getToken())
                        .withSecureConnectBundle(getSecureConnectBundleFile())
                        .withKeyspace(TEST_KEYSPACE)
                        .withEntity(SimpleDataEntity.class));
        pipelineWrite.run().waitUntilFinish();
        LOGGER.info("+ Loaded in {} ms", System.currentTimeMillis() - top);
        Assert.assertEquals(recordCount, getSession().execute("SELECT COUNT(*) FROM "
                + TEST_KEYSPACE + "." + SimpleDataEntity.TABLE_NAME).one()
                .getInt("count"));
    }

    @Test
    public void shouldReadFromAstra() {
        LOGGER.info("READ DATA FROM ASTRA");
        pipelineRead = TestPipeline.create();
        LOGGER.info("+ Pipeline created");
        long top = System.currentTimeMillis();
        PCollection<SimpleDataEntity> simpleDataPCollection =
                pipelineRead.apply(org.apache.beam.sdk.io.astra.AstraIO.<SimpleDataEntity>read()
                                .withToken(getToken())
                                .withSecureConnectBundle(getSecureConnectBundleFile())
                                .withKeyspace(TEST_KEYSPACE)
                                .withTable(SimpleDataEntity.TABLE_NAME)
                                .withMinNumberOfSplits(50)
                                .withCoder(SerializableCoder.of(SimpleDataEntity.class))
                                .withEntity(SimpleDataEntity.class));
        // Results ?
        PAssert.thatSingleton(simpleDataPCollection.apply("Count", Count.globally())).isEqualTo(2L);
        LOGGER.info("Done in {} ms", System.currentTimeMillis() - top);
    }

    @AfterClass
    public static void afterClass() {
        cleanup();
    }

}
