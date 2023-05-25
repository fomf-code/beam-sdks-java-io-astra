package org.apache.beam.sdk.io.astra.db;

import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.astra.AbstractAstraTest;
import org.apache.beam.sdk.io.astra.db.options.AstraDbReadOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PipelineOptionsValidator;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.values.PCollection;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * Test as a Pipeline
 *
 * To run the test:

 mvn test -Dtest=org.apache.beam.sdk.io.astra.db.AstraDbIOTestIT -DbeamTestPipelineOptions='["--astraSecureConnectBundle=/Users/cedricklunven/Downloads
 /scb-demo.zip","--astraToken=AstraCS:uZclXTYecCAqPPjiNmkezapR:e87d6edb702acd87516e4ef78e0c0e515c32ab2c3529f5a3242688034149a0e4","--table=scientist","--keyspace=demo"]' -DintegrationTestRunner=direct

 */
@Ignore
public class AstraDbIOTestIT extends AbstractAstraTest {

    /** Logger for the Class. */
    private static final Logger LOG = LoggerFactory.getLogger(AstraDbIOTestIT.class);

    private static final long NUM_ROWS = 22L;

    /**
     * Specific Options for the Astra Pipeline
     */
    private static AstraDbReadOptions astraOptions;

    @Rule
    public transient TestPipeline pipelineWrite = TestPipeline.create();

    @Rule
    public transient TestPipeline pipelineRead = TestPipeline.create();

    @BeforeClass
    public static void setup() {
        PipelineOptionsFactory.register(AstraDbReadOptions.class);
        PipelineOptions options = TestPipeline.testingPipelineOptions().as(AstraDbReadOptions.class);
        astraOptions = PipelineOptionsValidator.validate(AstraDbReadOptions.class, options);
    }

    @Test
    public void testRead() throws Exception {
        PCollection<AstraDbIOTest.Scientist> output =
                pipelineRead.apply(
                        AstraDbIO.<AstraDbIOTest.Scientist>read()
                                .withToken(astraOptions.getAstraToken())
                                .withSecureConnectBundle(new File(astraOptions.getAstraSecureConnectBundle()))
                                .withKeyspace(astraOptions.getKeyspace())
                                .withTable(astraOptions.getTable())
                                .withMinNumberOfSplits(50)
                                .withCoder(SerializableCoder.of(AstraDbIOTest.Scientist.class))
                                .withEntity(AstraDbIOTest.Scientist.class));
        PAssert.thatSingleton(output.apply("Count", Count.globally())).isEqualTo(NUM_ROWS);
        pipelineRead.run();
    }

}
