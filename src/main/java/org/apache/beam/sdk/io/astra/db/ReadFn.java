/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.metadata.token.Token;
import com.datastax.oss.driver.api.core.servererrors.SyntaxError;
import com.datastax.oss.driver.internal.core.metadata.token.Murmur3Token;
import org.apache.beam.sdk.io.astra.db.AstraDbIO.Read;
import org.apache.beam.sdk.io.astra.db.mapping.AstraDbMapper;
import org.apache.beam.sdk.io.astra.db.transforms.split.RingRange;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Joiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.Iterator;
import java.util.stream.Collectors;

/**
 * Read Data coming from Cassandra.
 *
 * @param <T>
 *        type entity manipulated
 */
class ReadFn<T> extends DoFn<Read<T>, T> {

  /** Logger for the class. */
  private static final Logger LOG = LoggerFactory.getLogger(ReadFn.class);

  /** Reader function. */
  public ReadFn() {
    super();
    LOG.info("Starter Reader");
  }

  @ProcessElement
  public void processElement(@Element Read<T> read, OutputReceiver<T> receiver) {
    try {
      if (read.ringRanges() != null) {
        LOG.debug("Read Table '{}.{}' ({} range)", read.keyspace().get(), read.table().get(), read.ringRanges().get().size());
      } else {
        LOG.debug("Read Table '{}.{}'", read.keyspace().get(), read.table().get());
      }
      CqlSession cqlSession = CqlSessionHolder.getCqlSession(read);
      AstraDbMapper<T> mapper = read.mapperFactoryFn().apply(cqlSession);
      Metadata clusterMetadata = cqlSession.getMetadata();
      KeyspaceMetadata keyspaceMetadata = clusterMetadata.getKeyspace(read.keyspace().get()).get();
      TableMetadata tableMetadata = keyspaceMetadata.getTable(read.table().get()).get();
      String partitionKey = tableMetadata.getPartitionKey().stream()
              .map(ColumnMetadata::getName)
              .map(CqlIdentifier::toString)
              .collect(Collectors.joining(","));

      String query = generateRangeQuery(read, partitionKey, read.ringRanges() != null);
      PreparedStatement preparedStatement = cqlSession.prepare(query);

      if (read.ringRanges() == null || read.ringRanges().get().isEmpty()) {
        // No Range, executing the full thing
        if (read.query() != null) {
          LOG.info("Executing User Query: {}", read.query().get());
          outputResults(cqlSession.execute(read.query().get()), receiver, mapper);
        } else {
          LOG.info("Executing Table Full Scan Query");
          outputResults(cqlSession.execute(String.format("SELECT * FROM %s.%s",
                  read.keyspace().get(),
                  read.table().get())), receiver, mapper);
        }
      } else {
        for (RingRange rr : read.ringRanges().get()) {
          Token startToken = new Murmur3Token(rr.getStart().longValue());
          Token endToken = new Murmur3Token(rr.getEnd().longValue());
          if (rr.isWrapping()) {
            ResultSet rsLow = cqlSession.execute(getLowestSplitQuery(read, partitionKey, rr.getEnd()));
            LOG.debug("Anything below [{}] get you {} items", rr.getEnd(), + rsLow.getAvailableWithoutFetching());
            outputResults(rsLow, receiver, mapper);

            ResultSet rsHigh = cqlSession.execute(getHighestSplitQuery(read, partitionKey, rr.getStart()));
            LOG.debug("Anything above [{}] get you {} items", rr.getStart(), + rsHigh.getAvailableWithoutFetching());
            outputResults(rsHigh, receiver,mapper);
          } else {
            ResultSet rsRange = cqlSession.execute(preparedStatement.bind()
                    .setToken(0, startToken)
                    .setToken(1, endToken));
            LOG.debug("Range[{}-{}] gets you {} items", rr.getStart().longValue(), rr.getEnd().longValue(), rsRange.getAvailableWithoutFetching());
            outputResults(rsRange, receiver, mapper);
          }
        }
      }
    } catch(SyntaxError se) {
        // The last token is not a valid token, so we need to wrap around
        // mismatched input 'AND' expecting EOF (...(person_name) from beam.scientist [AND]...)
        LOG.debug("SyntaxError : {}", se.getMessage());
    } catch (Exception ex) {
        LOG.error("Cannot process read operation against Cassandra", ex);
        throw new IllegalStateException("Cannot process read operation against Cassandra", ex);
    }
  }

  private static <T> void outputResults(ResultSet rs, OutputReceiver<T> outputReceiver, AstraDbMapper<T> mapper) {
    Iterator<T> iter = mapper.map(rs);
    while (iter.hasNext()) {
      T n = iter.next();
      outputReceiver.output(n);
    }
  }

  private static String getHighestSplitQuery(
      Read<?> spec, String partitionKey, BigInteger highest) {
    String highestClause = String.format("(token(%s) >= %d)",
            partitionKey, highest.subtract(new BigInteger("1")), partitionKey);
    String finalHighQuery =
        (spec.query() == null)
            ? buildInitialQuery(spec, true) + highestClause
            : spec.query() + " AND " + highestClause;
    return finalHighQuery + " ALLOW FILTERING";
  }

  private static String getLowestSplitQuery(Read<?> spec, String partitionKey, BigInteger lowest) {
    String lowestClause = String.format(" (token(%s) < %d) ", partitionKey, lowest.add(new BigInteger("1")));
    String finalLowQuery =
        (spec.query() == null)
            ? buildInitialQuery(spec, true) + lowestClause
            : spec.query() + " AND " + lowestClause;
    return finalLowQuery + " ALLOW FILTERING";
  }

  private static String generateRangeQuery(Read<?> spec, String partitionKey, Boolean hasRingRange) {
    final String rangeFilter =
        hasRingRange
            ? Joiner.on(" AND ")
                .skipNulls()
                .join(
                    String.format("(token(%s) >= ?)", partitionKey),
                    String.format("(token(%s) < ?)", partitionKey))
            : "";
    final String combinedQuery = buildInitialQuery(spec, hasRingRange) + rangeFilter + " ALLOW FILTERING";
    return combinedQuery;
  }

  private static String buildInitialQuery(Read<?> spec, Boolean hasRingRange) {
    String query = null;
    query = (spec.query() == null)
        ? String.format("SELECT * FROM %s.%s", spec.keyspace().get(), spec.table().get())
            + " WHERE "
        : spec.query().get()
            + (hasRingRange
                ? spec.query().get().toUpperCase().contains("WHERE") ? " AND " : " WHERE "
                : "");
    return query;
  }
}
