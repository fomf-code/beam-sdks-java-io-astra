package org.apache.beam.sdk.io.astra.db.vectorsearch;

import com.datastax.oss.driver.api.core.CqlSession;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.astra.AbstractAstraTest;
import org.apache.beam.sdk.io.astra.db.AstraDbIO;
import org.apache.beam.sdk.io.astra.db.AstraDbIOTest;
import org.apache.beam.sdk.io.astra.db.mapping.AstraDbMapper;
import org.apache.beam.sdk.io.astra.db.mapping.BeamRowMapperFactoryFn;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;

/**
 * Test the Connector on a Vector Enabled Astra DB.
 */
public class AstraDbIOVectorTest extends AbstractAstraTest implements Serializable {

    /** Test Constants. */
    private static final String DB_VECTOR_NAME   = "beam_sdk_vector";
    private static final String DB_KEYSPACE_NAME = "beam";

    @Rule
    public transient TestPipeline pipeline = TestPipeline.create();

    @BeforeClass
    public static void beforeClass() throws InterruptedException {
        createDbAndProvideClient(DB_VECTOR_NAME, DB_KEYSPACE_NAME, true);
        cqlSession = getCqlSession(DB_VECTOR_NAME, DB_KEYSPACE_NAME);
        createSchema();
        insertData();
    }

    @Test
    public void shouldBeConnected() throws IOException {
        // Should be connected
        Assert.assertNotNull(cqlSession);
        Assert.assertEquals(DB_KEYSPACE_NAME, cqlSession.getKeyspace().get().toString());

        SerializableFunction<CqlSession, AstraDbMapper<Row>> beamRowMapperFactory =
                new BeamRowMapperFactoryFn(DB_KEYSPACE_NAME, "products");

        // Accessing SCB Binary
        byte[] secureConnectBundle = Files.readAllBytes(getSecureConnectBundlePath(DB_VECTOR_NAME, DB_KEYSPACE_NAME));

        PCollection<String> output = pipeline.apply("Read From Cassandra",
                        AstraDbIO.<org.apache.beam.sdk.values.Row>read()
                                .withToken(getToken())
                                .withSecureConnectBundle(secureConnectBundle)
                                .withKeyspace(DB_KEYSPACE_NAME)
                                .withTable("products")
                                .withMinNumberOfSplits(3)
                                .withMapperFactoryFn(beamRowMapperFactory)
                                .withCoder(SerializableCoder.of(org.apache.beam.sdk.values.Row.class))
                                .withEntity(org.apache.beam.sdk.values.Row.class))
                .apply("Show", ParDo.of(new DoFn<Row, String>() {
                    @DoFn.ProcessElement
                    public void processElement(DoFn.ProcessContext c) {
                        System.out.println("Row:" + c.element().toString());
                        c.output(c.element().toString());
                    }
                }));
        pipeline.run();
    }

    private static void createSchema() {
        cqlSession.execute("" +
                "CREATE TABLE IF NOT EXISTS products (\n" +
                "  id int PRIMARY KEY,\n" +
                "  name TEXT,\n" +
                "  description TEXT,\n" +
                "  item_vector VECTOR<FLOAT, 5> )");
        cqlSession.execute("" +
                "CREATE CUSTOM INDEX IF NOT EXISTS ann_index " +
                "ON products(item_vector) " +
                "USING 'StorageAttachedIndex'");
    }

    private static void insertData() {
        cqlSession.execute("" +
                "INSERT INTO products (id, name, description, item_vector) " +
                "VALUES (1,'Coded Cleats','An AI quilt to help you sleep forever', " +
                "[0.1, 0.15, 0.3, 0.12, 0.05])");
        cqlSession.execute("" +
                "INSERT INTO products (id, name, description, item_vector) " +
                "VALUES (2,'Logic Layers','ChatGPT integrated sneakers that talk to you', " +
                "[0.45, 0.09, 0.01, 0.2, 0.11])");
        cqlSession.execute("" +
                "INSERT INTO products (id, name, description, item_vector) " +
                "VALUES (5,'Vision Vector Frame','A deep learning display that controls your mood', " +
                "[0.1, 0.05, 0.08, 0.3, 0.6])");
    }


}
