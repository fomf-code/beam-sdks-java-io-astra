package org.apache.beam.sdk.io.astra.db;

import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.mapping.annotations.ClusteringColumn;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.Computed;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.astra.AbstractAstraTest;
import org.apache.beam.sdk.io.astra.db.mapping.BeamRowObjectMapperFactory;
import org.apache.beam.sdk.io.astra.db.mapping.Mapper;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Objects;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.util.concurrent.ListeningExecutorService;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.util.concurrent.MoreExecutors;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(JUnit4.class)
public class AstraDbIOTest extends AbstractAstraTest implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(AstraDbIOTest.class);

    private static final long NUM_ROWS = 22L;

    private static final String CASSANDRA_KEYSPACE = "beam";

    private static final String CASSANDRA_TABLE = "scientist";

    private static final String CASSANDRA_TABLE_SIMPLEDATA = "simpledata";

    @Rule
    public transient TestPipeline pipeline = TestPipeline.create();

    @BeforeClass
    public static void beforeClass() {
        session = getSession();
        cluster = session.getCluster();
        insertData();
    }

    @AfterClass
    public static void afterClass() {
       cleanup();
    }

    private static void insertData() {
        LOG.info("Create Cassandra tables");
        session.execute(
                String.format(
                        "CREATE TABLE IF NOT EXISTS %s.%s(person_department text, person_id int, person_name text, PRIMARY KEY"
                                + "((person_department), person_id));",
                        CASSANDRA_KEYSPACE, CASSANDRA_TABLE));
        session.execute(
                String.format(
                        "CREATE TABLE IF NOT EXISTS %s.%s(person_department text, person_id int, person_name text, PRIMARY KEY"
                                + "((person_department), person_id));",
                        CASSANDRA_KEYSPACE, CASSANDRA_TABLE_WRITE));
        session.execute(
                String.format(
                        "CREATE TABLE IF NOT EXISTS %s.%s(id int, data text, PRIMARY KEY (id))",
                        CASSANDRA_KEYSPACE, CASSANDRA_TABLE_SIMPLEDATA));

        LOG.info("Insert records");
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
        for (int i = 0; i < NUM_ROWS; i++) {
            int index = i % scientists.length;
            String insertStr =
                    String.format(
                            "INSERT INTO %s.%s(person_department, person_id, person_name) values("
                                    + "'"
                                    + scientists[index][0]
                                    + "', "
                                    + i
                                    + ", '"
                                    + scientists[index][1]
                                    + "');",
                            CASSANDRA_KEYSPACE,
                            CASSANDRA_TABLE);
            session.execute(insertStr);
        }
        for (int i = 0; i < 100; i++) {
            String insertStr =
                    String.format(
                            "INSERT INTO %s.%s(id, data) VALUES(" + i + ",' data_" + i + "');",
                            CASSANDRA_KEYSPACE,
                            CASSANDRA_TABLE_SIMPLEDATA);
            session.execute(insertStr);
        }
    }

    /*
     Since we have enough data we will be able to detect if any get put in the ring range that wraps around
    */
    @Test
    public void testWrapAroundRingRanges() throws Exception {
        PCollection<SimpleData> simpledataPCollection =
                pipeline.apply(
                        AstraDbIO.<SimpleData>read()
                                .withToken(getToken())
                                .withSecureConnectBundle(getSecureConnectBundleFile())
                                .withKeyspace(CASSANDRA_KEYSPACE)
                                .withTable(CASSANDRA_TABLE_SIMPLEDATA)
                                .withMinNumberOfSplits(50)
                                .withCoder(SerializableCoder.of(SimpleData.class))
                                .withEntity(SimpleData.class));
        PCollection<Long> countPCollection = simpledataPCollection.apply("counting", Count.globally());
        PAssert.that(countPCollection)
                .satisfies(
                        i -> {
                            long total = 0;
                            for (Long aLong : i) {
                                total = total + aLong;
                            }
                            assertEquals(100, total);
                            return null;
                        });
        pipeline.run();
    }

    @Test
    public void testReadWithBeamObjectMapping() {

        SerializableFunction<Session, Mapper> beamRowMapperFactory =
                new BeamRowObjectMapperFactory(CASSANDRA_KEYSPACE, CASSANDRA_TABLE);

        PCollection<String> output = pipeline.apply("Read From Cassandra",
                        AstraDbIO.<org.apache.beam.sdk.values.Row>read()
                                .withToken(getToken())
                                .withSecureConnectBundle(getSecureConnectBundleFile())
                                .withKeyspace(CASSANDRA_KEYSPACE)
                                .withTable(CASSANDRA_TABLE)
                                .withMinNumberOfSplits(5)
                                .withMapperFactoryFn(beamRowMapperFactory)
                                .withCoder(SerializableCoder.of(org.apache.beam.sdk.values.Row.class))
                                .withEntity(org.apache.beam.sdk.values.Row.class))
                .apply("Show", ParDo.of(new ShowRow()));

        pipeline.run();
    }

    public static class ShowRow extends DoFn<org.apache.beam.sdk.values.Row, String> {

        @ProcessElement
        public void processElement(ProcessContext c) {
            LOG.info("Row: {}", c.element().toString());
            c.output(c.element().toString());
        }
    }


    @Test
    public void testRead() throws Exception {
        PCollection<Scientist> output =
                pipeline.apply(
                        AstraDbIO.<Scientist>read()
                                .withToken(getToken())
                                .withSecureConnectBundle(getSecureConnectBundleFile())
                                .withKeyspace(CASSANDRA_KEYSPACE)
                                .withTable(CASSANDRA_TABLE)
                                .withMinNumberOfSplits(50)
                                .withCoder(SerializableCoder.of(Scientist.class))
                                .withEntity(Scientist.class));

        PAssert.thatSingleton(output.apply("Count", Count.globally())).isEqualTo(NUM_ROWS);

        PCollection<KV<String, Integer>> mapped =
                output.apply(
                        MapElements.via(
                                new SimpleFunction<Scientist, KV<String, Integer>>() {
                                    @Override
                                    public KV<String, Integer> apply(Scientist scientist) {
                                        return KV.of(scientist.name, scientist.id);
                                    }
                                }));
        PAssert.that(mapped.apply("Count occurrences per scientist", Count.perKey()))
                .satisfies(
                        input -> {
                            int count = 0;
                            for (KV<String, Long> element : input) {
                                count++;
                                assertEquals(element.getKey(), NUM_ROWS / 10, element.getValue().longValue());
                            }
                            assertEquals(11, count);
                            return null;
                        });

        pipeline.run();
    }

    private AstraDbIO.Read<Scientist> getReadWithRingRange(RingRange... rr) {
        return AstraDbIO.<Scientist>read()
                .withToken(getToken())
                .withSecureConnectBundle(getSecureConnectBundleFile())
                .withRingRanges(new HashSet<>(Arrays.asList(rr)))
                .withKeyspace(CASSANDRA_KEYSPACE)
                .withTable(CASSANDRA_TABLE)
                .withCoder(SerializableCoder.of(Scientist.class))
                .withEntity(Scientist.class);
    }

    private AstraDbIO.Read<Scientist> getReadWithQuery(String query) {
        return AstraDbIO.<Scientist>read()
                .withToken(getToken())
                .withSecureConnectBundle(getSecureConnectBundleFile())
                .withQuery(query)
                .withKeyspace(CASSANDRA_KEYSPACE)
                .withTable(CASSANDRA_TABLE)
                .withCoder(SerializableCoder.of(Scientist.class))
                .withEntity(Scientist.class);
    }

    @Test
    public void testReadAllQuery() {
        String physQuery =
                String.format(
                        "SELECT * From %s.%s WHERE person_department='phys' AND person_id=0;",
                        CASSANDRA_KEYSPACE, CASSANDRA_TABLE);

        String mathQuery =
                String.format(
                        "SELECT * From %s.%s WHERE person_department='math' AND person_id=6;",
                        CASSANDRA_KEYSPACE, CASSANDRA_TABLE);

        PCollection<Scientist> output =
                pipeline
                        .apply(Create.of(getReadWithQuery(physQuery), getReadWithQuery(mathQuery)))
                        .apply(
                                AstraDbIO.<Scientist>readAll().withCoder(SerializableCoder.of(Scientist.class)));

        PCollection<String> mapped =
                output.apply(
                        MapElements.via(
                                new SimpleFunction<Scientist, String>() {
                                    @Override
                                    public String apply(Scientist scientist) {
                                        return scientist.name;
                                    }
                                }));
        PAssert.that(mapped).containsInAnyOrder("Einstein", "Newton");
        PAssert.thatSingleton(output.apply("count", Count.globally())).isEqualTo(2L);
        pipeline.run();
    }

    @Test
    public void testReadAllRingRange() {
        RingRange physRR =
                fromEncodedKey(
                        cluster.getMetadata(), TypeCodec.varchar().serialize("phys", ProtocolVersion.V3));

        RingRange mathRR =
                fromEncodedKey(
                        cluster.getMetadata(), TypeCodec.varchar().serialize("math", ProtocolVersion.V3));

        RingRange logicRR =
                fromEncodedKey(
                        cluster.getMetadata(), TypeCodec.varchar().serialize("logic", ProtocolVersion.V3));

        PCollection<Scientist> output =
                pipeline
                        .apply(Create.of(getReadWithRingRange(physRR), getReadWithRingRange(mathRR, logicRR)))
                        .apply(
                                AstraDbIO.<Scientist>readAll().withCoder(SerializableCoder.of(Scientist.class)));

        PCollection<KV<String, Integer>> mapped =
                output.apply(
                        MapElements.via(
                                new SimpleFunction<Scientist, KV<String, Integer>>() {
                                    @Override
                                    public KV<String, Integer> apply(Scientist scientist) {
                                        return KV.of(scientist.department, scientist.id);
                                    }
                                }));

        PAssert.that(mapped.apply("Count occurrences per department", Count.perKey()))
                .satisfies(
                        input -> {
                            HashMap<String, Long> map = new HashMap<>();
                            for (KV<String, Long> element : input) {
                                map.put(element.getKey(), element.getValue());
                            }
                            assertEquals(3, map.size()); // do we have all three departments
                            assertEquals(10L, (long) map.get("phys"));
                            assertEquals(4L, (long) map.get("math"));
                            assertEquals(2L, (long) map.get("logic"));
                            return null;
                        });

        pipeline.run();
    }

    @Test
    public void testReadWithQuery() throws Exception {
        String query =
                String.format(
                        "select person_id, writetime(person_name) from %s.%s where person_id=10 AND person_department='logic'",
                        CASSANDRA_KEYSPACE, CASSANDRA_TABLE);

        PCollection<Scientist> output =
                pipeline.apply(
                        AstraDbIO.<Scientist>read()
                                .withToken(getToken())
                                .withSecureConnectBundle(getSecureConnectBundleFile())
                                .withKeyspace(CASSANDRA_KEYSPACE)
                                .withTable(CASSANDRA_TABLE)
                                .withMinNumberOfSplits(20)
                                .withQuery(query)
                                .withCoder(SerializableCoder.of(Scientist.class))
                                .withEntity(Scientist.class));

        PAssert.thatSingleton(output.apply("Count", Count.globally())).isEqualTo(1L);
        PAssert.that(output)
                .satisfies(
                        input -> {
                            for (Scientist sci : input) {
                                assertNull(sci.name);
                                assertTrue(sci.nameTs != null && sci.nameTs > 0);
                            }
                            return null;
                        });

        pipeline.run();
    }

    @Test
    public void testReadWithUnfilteredQuery() throws Exception {
        String query =
                String.format(
                        "select person_id, writetime(person_name) from %s.%s",
                        CASSANDRA_KEYSPACE, CASSANDRA_TABLE);

        PCollection<Scientist> output =
                pipeline.apply(
                        AstraDbIO.<Scientist>read()
                                .withToken(getToken())
                                .withSecureConnectBundle(getSecureConnectBundleFile())
                                .withKeyspace(CASSANDRA_KEYSPACE)
                                .withTable(CASSANDRA_TABLE)
                                .withMinNumberOfSplits(20)
                                .withQuery(query)
                                .withCoder(SerializableCoder.of(Scientist.class))
                                .withEntity(Scientist.class));

        PAssert.thatSingleton(output.apply("Count", Count.globally())).isEqualTo(NUM_ROWS);
        PAssert.that(output)
                .satisfies(
                        input -> {
                            for (Scientist sci : input) {
                                assertNull(sci.name);
                                assertTrue(sci.nameTs != null && sci.nameTs > 0);
                            }
                            return null;
                        });

        pipeline.run();
    }

    @Test
    public void testWrite() {
        ArrayList<ScientistWrite> data = new ArrayList<>();
        for (int i = 0; i < NUM_ROWS; i++) {
            ScientistWrite scientist = new ScientistWrite();
            scientist.id = i;
            scientist.name = "Name " + i;
            scientist.department = "bio";
            data.add(scientist);
        }

        pipeline
                .apply(Create.of(data))
                .apply(
                        AstraDbIO.<ScientistWrite>write()
                                .withToken(getToken())
                                .withSecureConnectBundle(getSecureConnectBundleFile())
                                .withKeyspace(CASSANDRA_KEYSPACE)
                                .withEntity(ScientistWrite.class));
        // table to write to is specified in the entity in @Table annotation (in that case
        // scientist_write)
        pipeline.run();

        List<Row> results = getRows(CASSANDRA_TABLE_WRITE);
        assertEquals(NUM_ROWS, results.size());
        for (Row row : results) {
            assertTrue(row.getString("person_name").matches("Name (\\d*)"));
        }
    }

    private static final AtomicInteger counter = new AtomicInteger();

    private static class NOOPMapperFactory implements SerializableFunction<Session, Mapper> {

        @Override
        public Mapper apply(Session input) {
            return new NOOPMapper();
        }
    }

    private static class NOOPMapper implements Mapper<String>, Serializable {

        private final ListeningExecutorService executor =
                MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(10));

        final Callable<Void> asyncTask = () -> null;

        @Override
        public Iterator map(ResultSet resultSet) {
            if (!resultSet.isExhausted()) {
                resultSet.iterator().forEachRemaining(r -> counter.getAndIncrement());
            }
            return Collections.emptyIterator();
        }

        @Override
        public Future<Void> deleteAsync(String entity) {
            counter.incrementAndGet();
            return executor.submit(asyncTask);
        }

        @Override
        public Future<Void> saveAsync(String entity) {
            counter.incrementAndGet();
            return executor.submit(asyncTask);
        }
    }

    @Test
    public void testReadWithMapper() throws Exception {
        counter.set(0);

        SerializableFunction<Session, Mapper> factory = new NOOPMapperFactory();

        pipeline.apply(
                AstraDbIO.<String>read()
                        .withToken(getToken())
                        .withSecureConnectBundle(getSecureConnectBundleFile())
                        .withKeyspace(CASSANDRA_KEYSPACE)
                        .withTable(CASSANDRA_TABLE)
                        .withCoder(SerializableCoder.of(String.class))
                        .withEntity(String.class)
                        .withMapperFactoryFn(factory));
        pipeline.run();

        assertEquals(NUM_ROWS, counter.intValue());
    }

    @Test
    public void testCustomMapperImplWrite() throws Exception {
        counter.set(0);

        SerializableFunction<Session, Mapper> factory = new NOOPMapperFactory();

        pipeline
                .apply(Create.of(""))
                .apply(
                        AstraDbIO.<String>write()
                                .withToken(getToken())
                                .withSecureConnectBundle(getSecureConnectBundleFile())
                                .withKeyspace(CASSANDRA_KEYSPACE)
                                .withMapperFactoryFn(factory)
                                .withEntity(String.class));
        pipeline.run();

        assertEquals(1, counter.intValue());
    }

    @Test
    public void testCustomMapperImplDelete() {
        counter.set(0);

        SerializableFunction<Session, Mapper> factory = new NOOPMapperFactory();

        pipeline
                .apply(Create.of(""))
                .apply(
                        AstraDbIO.<String>delete()
                                .withToken(getToken())
                                .withSecureConnectBundle(getSecureConnectBundleFile())
                                .withKeyspace(CASSANDRA_KEYSPACE)
                                .withMapperFactoryFn(factory)
                                .withEntity(String.class));
        pipeline.run();

        assertEquals(1, counter.intValue());
    }

    private List<Row> getRows(String table) {
        ResultSet result =
                session.execute(
                        String.format("select person_id,person_name from %s.%s", CASSANDRA_KEYSPACE, table));
        return result.all();
    }

    @Test
    public void testDelete() throws Exception {
        List<Row> results = getRows(CASSANDRA_TABLE);
        assertEquals(NUM_ROWS, results.size());

        Scientist einstein = new Scientist();
        einstein.id = 0;
        einstein.department = "phys";
        einstein.name = "Einstein";
        pipeline
                .apply(Create.of(einstein))
                .apply(
                        AstraDbIO.<Scientist>delete()
                                .withToken(getToken())
                                .withSecureConnectBundle(getSecureConnectBundleFile())
                                .withKeyspace(CASSANDRA_KEYSPACE)
                                .withEntity(Scientist.class));

        pipeline.run();
        results = getRows(CASSANDRA_TABLE);
        assertEquals(NUM_ROWS - 1, results.size());
        // re-insert suppressed doc to make the test autonomous
        session.execute(
                String.format(
                        "INSERT INTO %s.%s(person_department, person_id, person_name) values("
                                + "'phys', "
                                + einstein.id
                                + ", '"
                                + einstein.name
                                + "');",
                        CASSANDRA_KEYSPACE,
                        CASSANDRA_TABLE));
    }

    /** Simple Cassandra entity used in read tests. */
    @Table(name = CASSANDRA_TABLE, keyspace = CASSANDRA_KEYSPACE)
    static class Scientist implements Serializable {

        @Column(name = "person_name")
        String name;

        @Computed("writetime(person_name)")
        Long nameTs;

        @ClusteringColumn()
        @Column(name = "person_id")
        int id;

        @PartitionKey
        @Column(name = "person_department")
        String department;

        @Override
        public String toString() {
            return id + ":" + name;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Scientist scientist = (Scientist) o;
            return id == scientist.id
                    && Objects.equal(name, scientist.name)
                    && Objects.equal(department, scientist.department);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(name, id);
        }
    }

    @Table(name = CASSANDRA_TABLE_SIMPLEDATA, keyspace = CASSANDRA_KEYSPACE)
    static class SimpleData implements Serializable {
        @PartitionKey int id;

        @Column String data;

        @Override
        public String toString() {
            return id + ", " + data;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            SimpleData that = (SimpleData) o;
            return id == that.id && data.equals(that.data);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(id, data);
        }
    }

    private static RingRange fromEncodedKey(Metadata metadata, ByteBuffer... bb) {
        BigInteger bi = BigInteger.valueOf((long) metadata.newToken(bb).getValue());
        return RingRange.of(bi, bi.add(BigInteger.valueOf(1L)));
    }

    private static final String CASSANDRA_TABLE_WRITE = "scientist_write";
    /** Simple Cassandra entity used in write tests. */
    @Table(name = CASSANDRA_TABLE_WRITE, keyspace = CASSANDRA_KEYSPACE)
    static class ScientistWrite extends Scientist {}
}
