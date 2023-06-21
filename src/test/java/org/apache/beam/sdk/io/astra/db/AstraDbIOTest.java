package org.apache.beam.sdk.io.astra.db;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.astra.AbstractAstraTest;
import org.apache.beam.sdk.io.astra.db.mapping.AstraDbMapper;
import org.apache.beam.sdk.io.astra.db.mapping.BeamRowDbMapperFactoryFn;
import org.apache.beam.sdk.io.astra.db.scientist.Scientist;
import org.apache.beam.sdk.io.astra.db.scientist.ScientistMapperFactoryFn;
import org.apache.beam.sdk.io.astra.db.simpledata.SimpleData;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.util.concurrent.ListeningExecutorService;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.util.concurrent.MoreExecutors;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.runners.MethodSorters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Testing the @see {@link AstraDbIO} connector.
 */
@RunWith(JUnit4.class)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class AstraDbIOTest extends AbstractAstraTest implements Serializable {

    /**
     * logger for the class
     */
    private static final Logger LOG = LoggerFactory.getLogger(AstraDbIOTest.class);

    /** Test Constants. */
    private static final String  TEST_DB           = "beam_sdk_integration_test";
    private static final String  TEST_KEYSPACE     = "beam";
    private static final String  TEST_TABLE        = "scientist";
    private static final String  TEST_TABLE_SIMPLE = "simpledata";

    private static final Long    SCIENTISTS_COUNT   = 22L;
    private static final Long    SIMPLE_DATA_COUNT  = 100L;
    private static final Integer TEST_SPLIT_COUNT   = 20;

    /** Test Pipeline. */
    @Rule
    public transient TestPipeline pipeline = TestPipeline.create();

    @Rule
    public transient TestPipeline pipelineDelete = TestPipeline.create();

    @BeforeClass
    public static void beforeClass() {
        cqlSession = getCqlSession(TEST_DB, TEST_KEYSPACE);
        createTables();
        insertScientist(SCIENTISTS_COUNT.intValue());
        insertSimpleData(SIMPLE_DATA_COUNT.intValue());
    }

    @AfterClass
    public static void afterClass() {
       if (cqlSession != null) {
           cqlSession.close();
       }
    }

    @Test
    public void test01ReadScientists() throws Exception {
        AstraDbIO.Read<Scientist> readScientistIO = AstraDbIO.<Scientist>read()
                .withToken(getToken())
                .withSecureConnectBundle(getSecureBundle())
                .withKeyspace(TEST_KEYSPACE)
                .withTable(Scientist.TABLE_NAME)
                .withMinNumberOfSplits(TEST_SPLIT_COUNT)
                .withEntity(Scientist.class)
                .withMapperFactoryFn(new ScientistMapperFactoryFn())
                .withCoder(SerializableCoder.of(Scientist.class));

        PCollection<Scientist> output = pipeline.apply(readScientistIO);
        PAssert.thatSingleton(output.apply("Count", Count.globally())).isEqualTo(SCIENTISTS_COUNT);

        PCollection<KV<String, Integer>> mapped = output.apply(
                MapElements.via(
                        new SimpleFunction<Scientist, KV<String, Integer>>() {
                            @Override
                            public KV<String, Integer> apply(Scientist scientist) {
                                return KV.of(scientist.getName(), scientist.getId());
                            }
                        }));
        PAssert.that(mapped.apply("Count occurrences per scientist", Count.perKey()))
                .satisfies(
                        input -> {
                            int count = 0;
                            for (KV<String, Long> element : input) {
                                count++;
                                assertEquals(element.getKey(), SCIENTISTS_COUNT / 10, element.getValue().longValue());
                            }
                            assertEquals(11, count);
                            return null;
                        });
        pipeline.run();
    }

    @Test
    public void test02ReadSimpleData() {

        // Manual Mapping
        SerializableFunction<CqlSession, AstraDbMapper<SimpleData>> customMapperFn =
                new SerializableFunction<>() {
            @Override
            public AstraDbMapper<SimpleData> apply(CqlSession cqlSession) {
                return new AstraDbMapper<SimpleData>() {

                    @Override
                    public SimpleData mapRow(Row row) {
                        SimpleData sd = new SimpleData();
                        sd.setId(row.getInt("id"));
                        sd.setData(row.getString("data"));
                        return sd;
                    }

                    @Override
                    public CompletionStage<Void> deleteAsync(SimpleData entity) {
                        String cqlQuery = String.format("DELETE FROM %s WHERE id = ?", SimpleData.TABLE_NAME);
                        return cqlSession
                                .executeAsync(SimpleStatement.newInstance(cqlQuery, entity.getId()))
                                .thenAccept(rs -> {});
                    }

                    @Override
                    public CompletionStage<Void> saveAsync(SimpleData entity) {
                        String cqlQuery = String.format("INSERT INTO %s(id,data) VALUES(?,?)", SimpleData.TABLE_NAME);
                        return cqlSession
                                .executeAsync(SimpleStatement.newInstance(cqlQuery, entity.getId(), entity.getData()))
                                .thenAccept(rs -> {});
                    }
                };
            }
        };

        AstraDbIO.Read<SimpleData> read = AstraDbIO.<SimpleData>read()
                .withToken(getToken())
                .withSecureConnectBundle(getSecureBundle())
                .withKeyspace(TEST_KEYSPACE)
                .withTable(SimpleData.TABLE_NAME)
                .withMinNumberOfSplits(TEST_SPLIT_COUNT)
                .withEntity(SimpleData.class)
                .withMapperFactoryFn(customMapperFn)
                .withCoder(SerializableCoder.of(SimpleData.class));

        // When running the pipeline
        PCollection<Long> countPCollection = pipeline
                .apply(read)
                .apply("counting", Count.globally());
        // Then, assert output
        PAssert.that(countPCollection).satisfies(i -> {
            long total = 0;
            for (Long aLong : i) {
                total = total + aLong;
            }
            assertEquals(SIMPLE_DATA_COUNT.longValue(), total);
            return null;
        });
        pipeline.run();
    }

    @Test
    public void test03ReadWithApacheBeamRowMapping() {
        PCollection<String> output = pipeline
                .apply("Read From Cassandra",
                        AstraDbIO.<org.apache.beam.sdk.values.Row>read()
                                .withToken(getToken())
                                .withSecureConnectBundle(getSecureBundle())
                                .withKeyspace(TEST_KEYSPACE)
                                .withTable(TEST_TABLE)
                                .withMinNumberOfSplits(TEST_SPLIT_COUNT)
                                .withMapperFactoryFn(new BeamRowDbMapperFactoryFn(TEST_KEYSPACE, TEST_TABLE))
                                .withCoder(SerializableCoder.of(org.apache.beam.sdk.values.Row.class))
                                .withEntity(org.apache.beam.sdk.values.Row.class))
                .apply("Show", ParDo.of(new ShowRow()));
        pipeline.run();
    }

    /**
     * Build a Read providing input Query on Scientist
     */
    private AstraDbIO.Read<Scientist> readScientistIO(String query) {
        return AstraDbIO.<Scientist>read()
                .withToken(getToken())
                .withSecureConnectBundle(getSecureBundle())
                .withKeyspace(TEST_KEYSPACE)
                .withTable(Scientist.TABLE_NAME)
                .withMinNumberOfSplits(20)
                .withQuery(query)
                .withMapperFactoryFn(new ScientistMapperFactoryFn())
                .withCoder(SerializableCoder.of(Scientist.class))
                .withEntity(Scientist.class);
    }

    @Test
    public void test04ReadAllQuery() throws Exception {
        String physQuery = String.format(
                        "SELECT * From %s.%s WHERE person_department='phys' AND person_id=0;",
                        TEST_KEYSPACE, TEST_TABLE);
        String mathQuery = String.format(
                        "SELECT * From %s.%s WHERE person_department='math' AND person_id=6;",
                        TEST_KEYSPACE, TEST_TABLE);
        PCollection<Scientist> output = pipeline
                .apply(Create.of(readScientistIO(physQuery), readScientistIO(mathQuery)))
                .apply(AstraDbIO.<Scientist>readAll().withCoder(SerializableCoder.of(Scientist.class)));
        PCollection<String> mapped = output.apply(MapElements.via(
                new SimpleFunction<Scientist, String>() {
                    @Override
                    public String apply(Scientist scientist) {
                        return scientist.getName();
        }
        }));
        PAssert.that(mapped).containsInAnyOrder("Einstein", "Newton");
        PAssert.thatSingleton(output.apply("count", Count.globally())).isEqualTo(2L);
        pipeline.run();
    }

    @Test
    public void test05ReadWithQuery() {
        String query = String.format(
                "SELECT * FROM %s.%s " +
                "WHERE person_id=10 " +
                "AND person_department='logic'", TEST_KEYSPACE, Scientist.TABLE_NAME);
        PCollection<Scientist> output = pipeline.apply(readScientistIO(query));
        PAssert.thatSingleton(output.apply("Count", Count.globally())).isEqualTo(1L);
        pipeline.run();
    }

    @Test
    public void test06ReadWithUnfilteredQuery() throws Exception {
        String selectAll = String.format( "select * from %s.%s",TEST_KEYSPACE, Scientist.TABLE_NAME);
        PCollection<Scientist> output = pipeline.apply(readScientistIO(selectAll));
        PAssert.thatSingleton(output.apply("Count", Count.globally())).isEqualTo(SCIENTISTS_COUNT);
        PAssert.that(output).satisfies(input -> {
            input.forEach(sci -> assertNotNull(sci.getName()));
            return null;
        });
        pipeline.run();
    }

    public void test07WriteAndDelete() throws IOException, InterruptedException {
        // Given a Table with records
        assertEquals((long) SCIENTISTS_COUNT, getRows(Scientist.TABLE_NAME).size());
        // Building a list of X new Records
        int newRecordsCount = 10;
        ArrayList<Scientist> data = new ArrayList<>();
        for (int i = SCIENTISTS_COUNT.intValue(); i < SCIENTISTS_COUNT.intValue()+newRecordsCount; i++) {
            Scientist scientist = new Scientist();
            scientist.setId(i);
            scientist.setName("Writing " + i);
            scientist.setDepartment("IT");
            data.add(scientist);
        }
        // Writing
        pipeline.apply(Create.of(data)).apply(
             AstraDbIO.<Scientist>write()
                      .withToken(getToken())
                      .withSecureConnectBundle(getSecureBundle())
                      .withKeyspace(TEST_KEYSPACE)
                      .withMapperFactoryFn(new ScientistMapperFactoryFn())
                      .withEntity(Scientist.class));
        pipeline.run();
        // Then X records have been added
        assertEquals((long) SCIENTISTS_COUNT+newRecordsCount, getRows(Scientist.TABLE_NAME).size());

        pipelineDelete.apply(Create.of(data)).apply(
                AstraDbIO.<Scientist>delete()
                        .withToken(getToken())
                        .withSecureConnectBundle(getSecureBundle())
                        .withKeyspace(TEST_KEYSPACE)
                        .withMapperFactoryFn(new ScientistMapperFactoryFn())
                        .withEntity(Scientist.class));
        pipelineDelete.run().waitUntilFinish();
        assertEquals((long) SCIENTISTS_COUNT, getRows(Scientist.TABLE_NAME).size());
    }

    private static final AtomicInteger counter = new AtomicInteger();

    private static class NOOPMapperFactory implements SerializableFunction<CqlSession, AstraDbMapper<String>> {

        @Override
        public AstraDbMapper apply(CqlSession input) {
            return new NOOPMapper();
        }
    }

    private static class NOOPMapper implements AstraDbMapper<String>, Serializable {

        private final ListeningExecutorService executor = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(10));

        final Callable<Void> asyncTask = () -> null;

        @Override
        public String mapRow(Row row) {
            return null;
        }

        @Override
        public Iterator map(ResultSet resultSet) {
            resultSet.iterator().forEachRemaining(r -> counter.getAndIncrement());
            return Collections.emptyIterator();
        }

        @Override
        public CompletionStage<Void> deleteAsync(String entity) {
            counter.incrementAndGet();
            return CompletableFuture.supplyAsync(() -> {
                try {
                    return executor.submit(asyncTask).get(); // Get the result of the ListenableFuture
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            });
        }

        @Override
        public CompletionStage<Void> saveAsync(String entity) {
            counter.incrementAndGet();
            return CompletableFuture.supplyAsync(() -> {
                try {
                    return executor.submit(asyncTask).get(); // Get the result of the ListenableFuture
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }

    @Test
    public void test08ReadWithMapper() throws Exception {
        counter.set(0);
        pipeline.apply(
                AstraDbIO.<String>read()
                        .withToken(getToken())
                        .withSecureConnectBundle(getSecureBundle())
                        .withKeyspace(TEST_KEYSPACE)
                        .withTable(TEST_TABLE)
                        .withCoder(SerializableCoder.of(String.class))
                        .withEntity(String.class)
                        .withMapperFactoryFn(new NOOPMapperFactory()));
        pipeline.run();
        assertEquals((long) SCIENTISTS_COUNT, counter.intValue());
    }

    @Test
    public void test09CustomMapperImplWrite() throws Exception {
        counter.set(0);
        pipeline.apply(Create.of(""))
                .apply(AstraDbIO.<String>write()
                        .withToken(getToken())
                        .withSecureConnectBundle(getSecureBundle())
                        .withKeyspace(TEST_KEYSPACE)
                        .withMapperFactoryFn( new NOOPMapperFactory())
                        .withEntity(String.class));
        pipeline.run();
        assertEquals(1, counter.intValue());
    }

    @Test
    public void test10CustomMapperImplDelete() throws IOException {
        counter.set(0);
        pipeline.apply(Create.of(""))
                .apply(AstraDbIO.<String>delete()
                        .withToken(getToken())
                        .withSecureConnectBundle(getSecureBundle())
                        .withKeyspace(TEST_KEYSPACE)
                        .withMapperFactoryFn(new NOOPMapperFactory())
                        .withEntity(String.class));
        pipeline.run();
        assertEquals(1, counter.intValue());
    }

    // ----- Utility methods -------------------------------------------------

    public static class ShowRow extends DoFn<org.apache.beam.sdk.values.Row, String> {

        @ProcessElement
        public void processElement(ProcessContext c) {
            LOG.info("Row: {}", c.element().toString());
            c.output(c.element().toString());
        }
    }

    private List<Row> getRows(String table) {
        return cqlSession
                .execute( String.format("select person_id,person_name from %s.%s", TEST_KEYSPACE, table))
                .all();
    }

    private static void createTables() {
        LOG.info("Create Cassandra tables");
        cqlSession.execute(
                String.format(
                        "CREATE TABLE IF NOT EXISTS %s.%s(person_department text, person_id int, person_name text, PRIMARY KEY"
                                + "((person_department), person_id));",
                        TEST_KEYSPACE, TEST_TABLE));
        cqlSession.execute(
                String.format(
                        "CREATE TABLE IF NOT EXISTS %s.%s(id int, data text, PRIMARY KEY (id))",
                        TEST_KEYSPACE, TEST_TABLE_SIMPLE));
    }

    private static void insertSimpleData(int recordCount) {
        for (int i = 0; i < recordCount; i++) {
            cqlSession.execute(String.format("" +
                            "INSERT INTO %s.%s(id, data) " +
                            "VALUES(" + i + ",' data_" + i + "');", TEST_KEYSPACE, TEST_TABLE_SIMPLE));
        }
    }

    private static void insertScientist(int recordCount) {
        LOG.info("Insert records");
        cqlSession.execute("truncate " + TEST_KEYSPACE + "." + TEST_TABLE);
        String[][] scientists = {
                new String[] {"phys", "Einstein"},
                new String[] {"bio", "Darwin"},
                new String[] {"phys", "Copernicus"},
                new String[] {"bio", "Pasteur"},
                new String[] {"bio", "Curie"},
                new String[] {"phys", "Faraday"},
                new String[] {"math", "Newton"},
                new String[] {"phys", "Bohr"},
                new String[] {"phys", "Galileo"},
                new String[] {"math", "Maxwell"},
                new String[] {"logic", "Russel"},
        };
        for (int i = 0; i < recordCount; i++) {
            int index = i % scientists.length;
            cqlSession.execute(String.format(
                    "INSERT INTO %s.%s(person_department, person_id, person_name) values("
                            + "'"
                            + scientists[index][0]
                            + "', "
                            + i
                            + ", '"
                            + scientists[index][1]
                            + "');",
                    TEST_KEYSPACE,
                    TEST_TABLE));
        }

    }

    private static byte[] getSecureBundle() {
        return getSecureBundle(TEST_DB, TEST_KEYSPACE);
    }
}
