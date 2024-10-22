/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.astra.db.transforms.split;

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
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.TokenMap;
import com.datastax.oss.driver.api.core.metadata.token.TokenRange;
import com.datastax.oss.driver.internal.core.metadata.SniEndPoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Generates partitions for a given keyspace.
 */
public class PartitionGenerator {

  /**
   * Work with CQL and Astra.
   */
  private static final Logger LOG = LoggerFactory.getLogger(PartitionGenerator.class);

  /**
   * Current Keyspace
   */
  private final CqlIdentifier keyspace;

  /**
   * Token factory
   */
  private final AstraTokenFactory tokenFactory;

  /**
   * Token map
   */
  private final TokenMap tokenMap;

  /**
   * Describes the ring and returns a list of token ranges.
   *
   * @param keyspace
   *    keyspace identifier
   * @param tokenMap
   *    token map
   * @param tokenFactory
   *    token factory
   */
  public PartitionGenerator(CqlIdentifier keyspace, TokenMap tokenMap, AstraTokenFactory tokenFactory) {
    this.keyspace = keyspace;
    this.tokenMap = tokenMap;
    this.tokenFactory = tokenFactory;
  }

  /**
   * Partitions the entire ring into approximately {@code splitCount} splits.
   *
   * @param splitCount
   *    The desired number of splits.
   * @return
   *    list of splits
   */
  @Nonnull
  public List<AstraTokenRange> partition(int splitCount) {
    List<AstraTokenRange> tokenRanges = describeRing(splitCount);
    int endpointCount = (int) tokenRanges.stream().map(AstraTokenRange::replicas).distinct().count();
    int maxGroupSize = tokenRanges.size() / endpointCount;
    TokenRangeSplitter splitter = tokenFactory.splitter();
    List<AstraTokenRange> splits = splitter.split(tokenRanges, splitCount);
    checkRing(splits);
    TokenRangeClusterer clusterer = tokenFactory.clusterer();
    List<AstraTokenRange> groups = clusterer.group(splits, splitCount, maxGroupSize);
    checkRing(groups);
    LOG.info("Real number of splits: {}", groups.size());
    return groups;
  }

  /**
   * Describe the ring.
   *
   * @param splitCount
   *    number of splits
   * @return
   *    the list of token ranges
   */
  private List<AstraTokenRange> describeRing(int splitCount) {
    List<AstraTokenRange> ranges =
        tokenMap.getTokenRanges().stream().map(this::toAstraTokenRange).collect(Collectors.toList());
    if (splitCount == 1) {
      AstraTokenRange r = ranges.get(0);
      return Collections.singletonList(
          tokenFactory.range(tokenFactory.minToken(), tokenFactory.minToken(), r.replicas()));
    } else {
      return ranges;
    }
  }

  /**
   * Convert default to Astra Token Range.
   *
   * @param range
   *    default token range
   * @return
   *    astra token range
   */
  private AstraTokenRange toAstraTokenRange(TokenRange range) {
    Set<AstraTokenRangeEndpoint> replicas =
        tokenMap.getReplicas(keyspace, range).stream()
            .map(Node::getEndPoint)
            .map(SniEndPoint.class::cast)
            .map(AstraTokenRangeEndpoint::new)
            .collect(Collectors.toSet());
    return tokenFactory.range(range.getStart(), range.getEnd(), replicas);
  }

  /**
   * Validating the sum of splits is the total ring.
   *
   * @param splits
   *    list of splits
   */
  private void checkRing(List<AstraTokenRange> splits) {
    double sum = splits.stream().map(AstraTokenRange::fraction).reduce(0d, Double::sum);
    if (Math.rint(sum) != 1.0d) {
      throw new IllegalStateException(
          String.format(
              "Incomplete ring partition detected: %1.3f. "
                  + "This is likely a bug in Astra SDK IO, please report. "
                  + "Generated splits: %s.",
              sum, splits));
    }
  }
}
