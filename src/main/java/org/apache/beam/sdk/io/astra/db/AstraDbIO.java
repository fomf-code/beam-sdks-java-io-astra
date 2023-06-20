package org.apache.beam.sdk.io.astra.db;

/*-
 * #%L
 * Beam SDK for Astra
 * --
 * Copyright (C) 2023 DataStax
 * --
 * Licensed under the Apache License, Version 2.0
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import com.datastax.oss.driver.api.core.CqlSession;
import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.astra.db.mapping.AstraDbMapper;
import org.apache.beam.sdk.io.astra.db.transforms.split.AstraSplitFn;
import org.apache.beam.sdk.io.astra.db.transforms.split.RingRange;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.function.BiFunction;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

/**
 * An IO to read and write from/to Astra.
 */
public class AstraDbIO {

  /**
   * Work with CQL and Astra.
   */
  private static final Logger LOG = LoggerFactory.getLogger(AstraDbIO.class);

  /**
   * Hiding default constructor.
   */
  private AstraDbIO() {}

  /**
   * Provide a {@link Read} {@link PTransform}.
   *
   * @param <T>
   *    values used in the pipeline
   * @return
   *    a {@link Read} {@link PTransform} to read data from an Astra database.
   */
  public static <T> Read<T> read() {
    return new AutoValue_AstraDbIO_Read.Builder<T>().build();
  }

  /**
   * Provide a {@link ReadAll} {@link PTransform}.
   *
   * @param <T>
   *    values used in the pipeline
   * @return
   *    a {@link ReadAll} {@link PTransform} to read data from an Astra database.
   */
  public static <T> ReadAll<T> readAll() {
    return new AutoValue_AstraDbIO_ReadAll.Builder<T>().build();
  }

  /**
   * Provide a {@link Write} {@link PTransform}.
   *
   * @param <T>
   *    values used in the pipeline
   * @return
   *    a {@link Write} {@link PTransform} to write data to an Astra database.
   */
  public static <T> Write<T> write() {
    return Write.<T>builder(MutationType.WRITE).build();
  }

  /**
   * Provide a {@link Write} {@link PTransform}.
   *
   * @param <T>
   *    values used in the pipeline
   * @return
   *    a {@link Write} {@link PTransform} to delete data to a Cassandra database.
   */
  public static <T> Write<T> delete() {
    return Write.<T>builder(MutationType.DELETE).build();
  }

  /**
   * A {@link PTransform} to read data from Apache Cassandra.
   * See {@link AstraDbIO} for more information on usage and configuration.
   * @param <T>
   *    Current type of values
   */
  @AutoValue
  @AutoValue.CopyAnnotations
  @SuppressWarnings({"rawtypes"})
  public abstract static class Read<T> extends PTransform<PBegin, PCollection<T>> {

    abstract @Nullable ValueProvider<String> token();

    abstract @Nullable ValueProvider<byte[]> secureConnectBundle();

    abstract @Nullable ValueProvider<String> keyspace();

    abstract @Nullable ValueProvider<String> table();

    abstract @Nullable ValueProvider<String> query();

    abstract @Nullable Class<T> entity();

    abstract @Nullable Coder<T> coder();

    public abstract @Nullable ValueProvider<Integer> minNumberOfSplits();

    abstract @Nullable SerializableFunction<CqlSession, AstraDbMapper<T>> mapperFactoryFn();

    abstract  @Nullable ValueProvider<List<RingRange>> ringRanges();

    abstract Builder<T> builder();

    /**
     * Specify the Cassandra keyspace where to read data.
     *
     * @param keyspace
     *    cassandra keyspace
     * @return
     *    current {@link Read} builder
     */
    public Read<T> withKeyspace(String keyspace) {
      checkArgument(keyspace != null, "keyspace can not be null");
      return withKeyspace(ValueProvider.StaticValueProvider.of(keyspace));
    }

    /**
     * Specify the Cassandra keyspace where to read data.
     *
     * @param keyspace
     *    cassandra keyspace
     * @return
     *    current {@link Read} builder
     */
    public Read<T> withKeyspace(ValueProvider<String> keyspace) {
      return builder().setKeyspace(keyspace).build();
    }

    /**
     * Specify the Cassandra table where to read data.
     *
     * @param table
     *    cassandra table
     * @return
     *    current {@link Read} builder
     */
    public Read<T> withTable(String table) {
      checkArgument(table != null, "table can not be null");
      return withTable(ValueProvider.StaticValueProvider.of(table));
    }

    /**
     * Specify the Cassandra table where to read data.
     *
     * @param table
     *    cassandra table
     * @return
     *    current {@link Read} builder
     */
    public Read<T> withTable(ValueProvider<String> table) {
      return builder().setTable(table).build();
    }

    /**
     * Specify the Cassandra query where to read data.
     *
     * @param query
     *    cassandra query
     * @return
     *    current {@link Read} builder
     */
    public Read<T> withQuery(String query) {
      checkArgument(query != null && query.length() > 0, "query cannot be null");
      return withQuery(ValueProvider.StaticValueProvider.of(query));
    }

    /**
     * Specify the Cassandra query where to read data.
     *
     * @param query
     *    cassandra query
     * @return
     *    current {@link Read} builder
     */
    public Read<T> withQuery(ValueProvider<String> query) {
      return builder().setQuery(query).build();
    }

    /**
     * Specify the entity class (annotated POJO). The {@link AstraDbIO} will read the data and
     * convert the data as entity instances. The {@link PCollection} resulting from the read will
     * contain entity elements.
     *
     * @param entity
     *    cassandra entity
     * @return
     *    current {@link Read} builder
     */
    public Read<T> withEntity(Class<T > entity) {
      checkArgument(entity != null, "entity can not be null");
      return builder().setEntity(entity).build();
    }

    /**
     * Specify the {@link Coder} used to serialize the entity in the {@link PCollection}.
     *
     * @param coder
     *    Apache Beam Coder
     * @return
     *    current {@link Read} builder
     */
    public Read<T> withCoder(Coder<T> coder) {
      checkArgument(coder != null, "coder can not be null");
      return builder().setCoder(coder).build();
    }

    /**
     * Specify the token used for authentication.
     *
     * @param token
     *    astra token
     * @return
     *    current {@link Read} builder
     */
    public Read<T> withToken(String token) {
      checkArgument(token != null, "token can not be null");
      return withToken(ValueProvider.StaticValueProvider.of(token));
    }

    /**
     * Specify the token used for authentication.
     *
     * @param token
     *    astra token
     * @return
     *    current {@link Read} builder
     */
    public Read<T> withToken(ValueProvider<String> token) {
      return builder().setToken(token).build();
    }

    /**
     * It's possible that system.size_estimates isn't populated or that the number of splits
     * computed by Beam is still to low for Cassandra to handle it. This setting allows to enforce a
     * minimum number of splits in case Beam cannot compute it correctly.
     *
     * @param minNumberOfSplits
     *    number of splits for token range computation
     * @return
     *    current {@link Read} builder
     */
    public Read<T> withMinNumberOfSplits(Integer minNumberOfSplits) {
      checkArgument(minNumberOfSplits != null, "minNumberOfSplits can not be null");
      checkArgument(minNumberOfSplits > 0, "minNumberOfSplits must be greater than 0");
      return withMinNumberOfSplits(ValueProvider.StaticValueProvider.of(minNumberOfSplits));
    }

    /**
     * It's possible that system.size_estimates isn't populated or that the number of splits
     * computed by Beam is still to low for Cassandra to handle it. This setting allows to enforce a
     * minimum number of splits in case Beam cannot compute it correctly.
     *
     * @param minNumberOfSplits
     *    number of splits for token range computation
     * @return
     *    current {@link Read} builder
     */
    public Read<T> withMinNumberOfSplits(ValueProvider<Integer> minNumberOfSplits) {
      return builder().setMinNumberOfSplits(minNumberOfSplits).build();
    }

    /**
     * Specify ring ranges explicitly.
     *
     * @param ringRange
     *    ring range
     * @return
     *    current {@link Read} builder
     */
    public Read<T> withRingRanges(List<RingRange> ringRange) {
      return withRingRanges(ValueProvider.StaticValueProvider.of(ringRange));
    }

    /**
     * Specify ring ranges explicitly.
     *
     * @param ringRange
     *    ring range
     * @return
     *    current {@link Read} builder
     */
    public Read<T> withRingRanges(ValueProvider<List<RingRange>> ringRange) {
      return builder().setRingRanges(ringRange).build();
    }

    /**
     * A factory to create a specific {@link AstraDbMapper} for a given Cassandra Session. This is useful
     * to provide mappers that don't rely upon Cassandra annotated objects.
     *
     * @param mapperFactory
     *    mapper Factory
     * @return
     *    current {@link Read} builder
     */
    public Read<T> withMapperFactoryFn(SerializableFunction<CqlSession, AstraDbMapper<T>> mapperFactory) {
      checkArgument(
          mapperFactory != null,
          "CassandraIO.withMapperFactory" + "(withMapperFactory) called with null value");
      return builder().setMapperFactoryFn(mapperFactory).build();
    }

    /**
     * Populate SCB as a file.
     *
     * @param scbFile
     *    secure connect bundle file
     * @return
     *    reference to READ
     */
    public Read<T> withSecureConnectBundle(byte[] scbFile) {
      checkArgument(scbFile != null, "keyspace can not be null");
      return withSecureConnectBundle(ValueProvider.StaticValueProvider.of(scbFile));
    }

    /**
     * Populate SCB as a file.
     *
     * @param cloudSecureConnectBundleFile
     *    secure connect bundle file
     * @return
     *    reference to READ
     */
    public Read<T> withSecureConnectBundle(ValueProvider<byte[]> cloudSecureConnectBundleFile) {
      checkArgument(cloudSecureConnectBundleFile != null, "keyspace can not be null");
      return builder().setSecureConnectBundle(cloudSecureConnectBundleFile).build();
    }

    /**
     * Read function to start pipeline.
     * @param input
     *    starter for the pipeline
     * @return
     *    collection of values.
     */
    @Override
    public PCollection<T> expand(PBegin input) {
      checkArgument(token() != null, "withToken() is required");
      checkArgument(keyspace() != null, "withKeyspace() is required");
      checkArgument(table() != null || query() != null, "wihtTable() or withQuery() is required");
      checkArgument(entity() != null, "withEntity() is required");
      checkArgument(coder() != null, "withCoder() is required");
      checkArgument(secureConnectBundle() != null,"secure connect bundle is required");
      checkArgument(mapperFactoryFn() != null, "withMapperFactoryFn() is required");
      return input
              .apply(Create.of(this))
              .apply("Create Splits", ParDo.of(new AstraSplitFn<T>()))
              .setCoder(SerializableCoder.of(new TypeDescriptor<Read<T>>() {}))
              .apply("ReadAll", AstraDbIO.<T>readAll().withCoder(coder()));
    }

    @AutoValue.Builder
    abstract static class Builder<T> {

      abstract Builder<T> setSecureConnectBundle(ValueProvider<byte[]> scbStream);

      abstract Builder<T> setToken(ValueProvider<String> token);

      abstract Builder<T> setKeyspace(ValueProvider<String> keyspace);

      abstract Builder<T> setTable(ValueProvider<String> table);

      abstract Builder<T> setQuery(ValueProvider<String> query);

      abstract Builder<T> setEntity(Class<T> entity);

      abstract Optional<Class<T>> entity();

      abstract Builder<T> setCoder(Coder<T> coder);

      abstract Builder<T> setMinNumberOfSplits(ValueProvider<Integer> minNumberOfSplits);

      abstract Builder<T> setMapperFactoryFn(SerializableFunction<CqlSession, AstraDbMapper<T>> mapperFactoryFn);

      abstract Builder<T> setRingRanges(ValueProvider<List<RingRange>> ringRange);

      abstract Optional<SerializableFunction<CqlSession, AstraDbMapper<T>>> mapperFactoryFn();

      abstract Read<T> autoBuild();

      public Read<T> build() {
        return autoBuild();
      }

    }
  }

  /** Specify the mutation type: either write or delete. */
  public enum MutationType {

    /** Write mutation type. */
    WRITE,

    /** Delete mutation type. */
    DELETE
  }

  /**
   * A {@link PTransform} to mutate into Apache Cassandra. See {@link AstraDbIO} for details on
   * usage and configuration.
   */
  @AutoValue
  @AutoValue.CopyAnnotations
  @SuppressWarnings({"rawtypes"})
  public abstract static class Write<T> extends PTransform<PCollection<T>, PDone> {

    abstract @Nullable ValueProvider<String> token();

    abstract @Nullable ValueProvider<byte[]> secureConnectBundle();

    abstract @Nullable ValueProvider<String> keyspace();

    abstract @Nullable Class<T> entity();

    abstract MutationType mutationType();

    abstract @Nullable SerializableFunction<CqlSession, AstraDbMapper<T>> mapperFactoryFn();

    abstract Builder<T> builder();

    static <T> Builder<T> builder(MutationType mutationType) {
      return new AutoValue_AstraDbIO_Write.Builder<T>().setMutationType(mutationType);
    }

    /**
     * Specify the token used for authentication.
     *
     * @param token
     *    astra token
     * @return
     *    current {@link Write} builder
     */
    public Write<T> withToken(String token) {
      checkArgument(
              token != null,
              "AstraIO."
                      + getMutationTypeName()
                      + "().withToken(token) called with "
                      + "null token");
      return withToken(ValueProvider.StaticValueProvider.of(token));
    }

    /**
     * Specify the token used for authentication.
     *
     * @param token
     *    astra token
     * @return
     *    current {@link Write} builder
     */
    public Write<T> withToken(ValueProvider<String> token) {
      return builder().setToken(token).build();
    }

    /**
     * Specify the Cassandra keyspace where to write data.
     *
     * @param keyspace
     *    astra keyspace
     * @return
     *    current {@link Write} builder
     */
    public Write<T> withKeyspace(String keyspace) {
      checkArgument(
          keyspace != null,
          "CassandraIO."
              + getMutationTypeName()
              + "().withKeyspace(keyspace) called with "
              + "null keyspace");
      return withKeyspace(ValueProvider.StaticValueProvider.of(keyspace));
    }

    /**
     * Specify the Cassandra keyspace where to write data.
     *
     * @param keyspace
     *    astra keyspace
     * @return
     *    current {@link Write} builder
     */
    public Write<T> withKeyspace(ValueProvider<String> keyspace) {
      return builder().setKeyspace(keyspace).build();
    }

    /**
     * Specify the entity class in the input {@link PCollection}. The {@link AstraDbIO} will map
     * this entity to the Cassandra table thanks to the annotations.
     *
     * @param entity
     *    java entity
     * @return
     *    current {@link Write} builder
     */
    public Write<T> withEntity(Class<T> entity) {
      checkArgument(
          entity != null,
          "CassandraIO."
              + getMutationTypeName()
              + "().withEntity(entity) called with null "
              + "entity");
      return builder().setEntity(entity).build();
    }

    /**
     * Populate SCB as a stream.
     *
     * @param scbStream
     *    secure connect bundle file
     * @return
     *    reference to write
     */
    public Write<T> withSecureConnectBundle(byte[] scbStream) {
      checkArgument(scbStream != null, "scbStream cannot be null");
      return withSecureConnectBundle(ValueProvider.StaticValueProvider.of(scbStream));
    }

    /**
     * Populate SCB as a stream.
     *
     * @param scbStream
     *    secure connect bundle file
     * @return
     *    reference to write
     */
    public Write<T> withSecureConnectBundle(ValueProvider<byte[]> scbStream) {
      checkArgument(scbStream != null, "scbStream cannot be null");
      return builder().setSecureConnectBundle(scbStream).build();
    }

    /**
     * Specify the Mapper factory function.
     *
     * @param mapperFactoryFn
     *    current mapper factory function
     * @return
     *    reference to write
     */
    public Write<T> withMapperFactoryFn(SerializableFunction<CqlSession, AstraDbMapper<T>> mapperFactoryFn) {
      checkArgument(
          mapperFactoryFn != null,
          "AstraIO."
              + getMutationTypeName()
              + "().mapperFactoryFn"
              + "(mapperFactoryFn) called with null value");
      return builder().setMapperFactoryFn(mapperFactoryFn).build();
    }

    @Override
    public void validate(PipelineOptions pipelineOptions) {

      checkState(
              token() != null,
              "AstraIO."
                      + getMutationTypeName()
                      + "() requires a token to be set via "
                      + "withToken(token)");
      checkState(
          keyspace() != null,
          "AstraIO."
              + getMutationTypeName()
              + "() requires a keyspace to be set via "
              + "withKeyspace(keyspace)");
      checkState(
          entity() != null,
          "AstraIO."
              + getMutationTypeName()
              + "() requires an entity to be set via "
              + "withEntity(entity)");
    }

    @Override
    public PDone expand(PCollection<T> input) {
      if (mutationType() == MutationType.DELETE) {
        input.apply(ParDo.of(new DeleteFn<>(this)));
      } else {
        input.apply(ParDo.of(new WriteFn<>(this)));
      }
      return PDone.in(input.getPipeline());
    }

    private String getMutationTypeName() {
      return mutationType() == null
          ? MutationType.WRITE.name().toLowerCase()
          : mutationType().name().toLowerCase();
    }

    @AutoValue.Builder
    abstract static class Builder<T> {

      abstract Builder<T> setSecureConnectBundle(ValueProvider<byte[]> scbStream);

      abstract Builder<T> setToken(ValueProvider<String> token);

      abstract Builder<T> setKeyspace(ValueProvider<String> keyspace);

      abstract Builder<T> setEntity(Class<T> entity);

      abstract Optional<Class<T>> entity();

      abstract Builder<T> setMutationType(MutationType mutationType);

      abstract Builder<T> setMapperFactoryFn(SerializableFunction<CqlSession, AstraDbMapper<T>> mapperFactoryFn);

      abstract Optional<SerializableFunction<CqlSession, AstraDbMapper<T>>> mapperFactoryFn();

      abstract Write<T> autoBuild(); // not public

      public Write<T> build() {
        return autoBuild();
      }
    }
  }

  private static class WriteFn<T> extends DoFn<T, Void> {
    private final Write<T> spec;
    private transient Mutator<T> writer;

    WriteFn(Write<T> spec) {
      this.spec = spec;
    }

    @Setup
    public void setup() {
      writer = new Mutator<>(spec, AstraDbMapper::saveAsync, "writes");
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws ExecutionException, InterruptedException {
      writer.mutate(c.element());
    }

    @FinishBundle
    public void finishBundle() throws Exception {
      writer.flush();
    }

    @Teardown
    public void teardown() throws Exception {
      writer = null;
    }
  }

  private static class DeleteFn<T> extends DoFn<T, Void> {
    private final Write<T> spec;
    private transient Mutator<T> deleter;

    DeleteFn(Write<T> spec) {
      this.spec = spec;
    }

    @Setup
    public void setup() {
      deleter = new Mutator<>(spec, AstraDbMapper::deleteAsync, "deletes");
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws ExecutionException, InterruptedException {
      deleter.mutate(c.element());
    }

    @FinishBundle
    public void finishBundle() throws Exception {
      deleter.flush();
    }

    @Teardown
    public void teardown() throws Exception {
      deleter = null;
    }
  }

  /**
   * Mutator allowing to do side effects into Apache Cassandra database.
   **/
  private static class Mutator<T> {

    /**
     * The threshold of 100 concurrent async queries is a heuristic commonly used by the Apache
     * Cassandra community. There is no real gain to expect in tuning this value.
     */
    private static final int CONCURRENT_ASYNC_QUERIES = 100;

    /**
     * Cassandra session to use.
     */
    private final CqlSession session;

    /**
     * The mapper factory function to use.
     */
    private final SerializableFunction<CqlSession, AstraDbMapper<T>> mapperFactoryFn;

    /**
     * The futures of the current batch of mutations.
     */
    private List<CompletionStage<Void>> mutateFutures;

    /**
     * The function to use to mutate the entity.
     */
    private final BiFunction<AstraDbMapper<T>, T, CompletionStage<Void>> mutator;

    /**
     * The name of the operation to perform.
     */
    private final String operationName;

    /**
     * Creates a new {@link Mutator} instance.
     *
     * @param spec The {@link Write} specification.
     * @param mutator The function to use to mutate the entity.
     * @param operationName The name of the operation to perform.
     */
    Mutator(Write<T> spec, BiFunction<AstraDbMapper<T>, T, CompletionStage<Void>> mutator, String operationName) {
      this.session         = CqlSessionHolder.getCqlSession(spec);
      this.mapperFactoryFn = spec.mapperFactoryFn();
      this.mutateFutures   = new ArrayList<>();
      this.mutator         = mutator;
      this.operationName   = operationName;
    }

    /**
     * Mutate the entity to the Cassandra instance.
     *
     * @param entity
     *    current entity
     * @throws ExecutionException
     *    error occurred at execution
     * @throws InterruptedException
     *    error occurred at execution
     */
    void mutate(T entity) throws ExecutionException, InterruptedException {
      AstraDbMapper<T> mapper = mapperFactoryFn.apply(session);
      this.mutateFutures.add(mutator.apply(mapper, entity));
      if (this.mutateFutures.size() == CONCURRENT_ASYNC_QUERIES) {
        // We reached the max number of allowed in flight queries.
        // Write methods are synchronous in Beam,
        // so we wait for each async query to return before exiting.
        LOG.debug(
            "Waiting for a batch of {} Cassandra {} to be executed...",
            CONCURRENT_ASYNC_QUERIES,
            operationName);
        waitForFuturesToFinish();
        this.mutateFutures = new ArrayList<>();
      }
    }

    void flush() throws ExecutionException, InterruptedException {
      if (this.mutateFutures.size() > 0) {
        // Waiting for the last in flight async queries to return before finishing the bundle.
        waitForFuturesToFinish();
      }
    }

    private void waitForFuturesToFinish() throws ExecutionException, InterruptedException {
      for (CompletionStage<Void> future : mutateFutures) {
        future.toCompletableFuture().get();
      }
    }
  }

  /**
   * A {@link PTransform} to read data from Apache Cassandra. See {@link AstraDbIO} for more
   * information on usage and configuration.
   */
  @AutoValue
  public abstract static class ReadAll<T> extends PTransform<PCollection<Read<T>>, PCollection<T>> {
    @AutoValue.Builder
    abstract static class Builder<T> {

      abstract Builder<T> setCoder(Coder<T> coder);

      abstract ReadAll<T> autoBuild();

      public ReadAll<T> build() {
        return autoBuild();
      }
    }

    @Nullable
    abstract Coder<T> coder();

    abstract Builder<T> builder();

    /**
     * Specify the {@link Coder} used to serialize the entity in the {@link PCollection}.
     *
     * @param coder
     *    current coder
     * @return
     *   current builder
     */
    public ReadAll<T> withCoder(Coder<T> coder) {
      checkArgument(coder != null, "coder can not be null");
      return builder().setCoder(coder).build();
    }

    @Override
    public PCollection<T> expand(PCollection<Read<T>> input) {
      checkArgument(coder() != null, "withCoder() is required");
      return input
          .apply("Reshuffle", Reshuffle.viaRandomKey())
          .apply("Read", ParDo.of(new ReadFn<>()))
          .setCoder(this.coder());
    }
  }

}
