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

import com.datastax.oss.driver.api.core.metadata.token.Token;
import com.datastax.oss.driver.api.core.metadata.token.TokenRange;
import com.datastax.oss.driver.shaded.guava.common.collect.ComparisonChain;
import com.datastax.oss.driver.shaded.guava.common.collect.Lists;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * Groups small, contiguous token ranges sharing the same replicas in order to reduce the total
 * number of splits.
 *
 * <p>Useful mostly with virtual nodes, which may create lots of small token range splits.
 */
public class TokenRangeClusterer {

  /**
   * Reference to the factory
   */
  private final AstraTokenFactory tokenFactory;

  /**
   * Constructor for the clusterer.
   *
   * @param tokenFactory
   *    list of token
   */
  public TokenRangeClusterer(@Nonnull AstraTokenFactory tokenFactory) {
    this.tokenFactory = tokenFactory;
  }

  /**
   * Groups contiguous ranges together as long as they are contiguous and share the same replicas.
   *
   * <p>Note: the grouping algorithm used in DSBulk is different from the one used by the Spark
   * connector: the former favors groups of contiguous ranges sharing the same replicas (in order to
   * make it possible to route a range read to a coordinator that is also a replica), whereas the
   * latter favors data locality (i.e. groups even non-continguous ranges as long as they share at
   * least one common replica).
   *
   * @param ranges
   *    list of ranges
   * @param groupCount
   *    number of gorups
   * @param maxGroupSize
   *    maximum group size
   * @return
   *    list of token
   */
  @Nonnull
  public List<AstraTokenRange> group(List<AstraTokenRange> ranges, int groupCount, int maxGroupSize) {
    double ringFractionPerGroup = 1.0d / groupCount;
    LinkedList<AstraTokenRange> sorted = Lists.newLinkedList(ranges);
    sorted.sort(
        (tr1, tr2) ->
            ComparisonChain.start()
                .compare(tr1.getStart(), tr2.getStart())
                .compare(tr1.getEnd(), tr2.getEnd())
                .result());
    if (sorted.isEmpty()) {
      return sorted;
    }
    List<AstraTokenRange> grouped = new ArrayList<>();
    while (!sorted.isEmpty()) {
      AstraTokenRange head = sorted.peek();
      assert head != null;
      double ringFractionLimit =
          Math.max(
              ringFractionPerGroup,
              head.fraction()); // make sure first element will be always included
      double cumulativeRingFraction = 0;
      Token end = head.getStart();
      for (int i = 0; i < Math.max(1, maxGroupSize) && !sorted.isEmpty(); i++) {
        AstraTokenRange current = sorted.peek();
        assert current != null;
        cumulativeRingFraction += current.fraction();
        // keep grouping ranges as long as they share the same replicas and the resulting
        // range is contiguous.
        if (cumulativeRingFraction > ringFractionLimit
            || !head.replicas().equals(current.replicas())
            || !end.equals(current.getStart())) {
          break;
        }
        sorted.pop();
        end = current.getEnd();
      }
      grouped.add(tokenFactory.range(head.getStart(), end, head.replicas()));
    }
    List<AstraTokenRange> list = new ArrayList<>();
    for (AstraTokenRange tr : grouped) {
      for (TokenRange r : tr.unwrap()) {
        list.add(tokenFactory.range(r.getStart(), r.getEnd(), tr.replicas()));
      }
    }
    return list;
  }
}
