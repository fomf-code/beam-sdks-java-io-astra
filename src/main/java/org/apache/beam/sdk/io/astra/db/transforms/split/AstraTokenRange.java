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

import com.datastax.oss.driver.internal.core.metadata.token.Murmur3Token;
import com.datastax.oss.driver.internal.core.metadata.token.Murmur3TokenRange;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.math.BigInteger;
import java.util.Set;

/**
 * Astra is using murmur3 partitioner, so we need to use the same partitioner to generate the token.
 */
public class AstraTokenRange extends Murmur3TokenRange implements Serializable {

  /** Set of replicas. */
  private final Set<AstraTokenRangeEndpoint> replicas;

  /**
   * Constructor without replicas.
   * @param start
   *      start token
   * @param end
   *      end token
   */
  public AstraTokenRange(@Nonnull Murmur3Token start, @Nonnull Murmur3Token end) {
    super(start, end);
    this.replicas = ImmutableSet.of();
  }

  /**
   * Full constructor.
   * @param start
   *    start token
   * @param end
   *    end token
   * @param replicas
   *    replicas
   */
  public AstraTokenRange(@Nonnull Murmur3Token start, @Nonnull Murmur3Token end, @Nonnull Set<AstraTokenRangeEndpoint> replicas) {
    super(start, end);
    this.replicas = ImmutableSet.copyOf(replicas);
  }

  /**
   * Start token
   *
   * @return
   *   getter for start token
   */
  @Nonnull
  public Murmur3Token getStart() {
    return (Murmur3Token) super.getStart();
  }

  /**
   * End token.
   *
   * @return
   *   getter for end token
   */
  @Nonnull
  public Murmur3Token getEnd() {
    return (Murmur3Token) super.getEnd();
  }

  /**
   * List of replicas.
   *
   * @return
   *    replicas list
   */
  @Nonnull
  public Set<AstraTokenRangeEndpoint> replicas() {
    return replicas;
  }

  /**
   * Size of the range.
   *
   * @return
   *    size
   */
  @Nonnull
  public BigInteger size() {
    BigInteger left = BigInteger.valueOf(getStart().getValue());
    BigInteger right = BigInteger.valueOf(getEnd().getValue());
    if (right.compareTo(left) > 0) {
      return right.subtract(left);
    } else {
      return right.subtract(left).add(AstraTokenFactory.TOTAL_TOKEN_COUNT);
    }
  }

  /**
   * Fraction of ring range.
   *
   * @return
   *    fracction of the range
   */
  public double fraction() {
    return size().doubleValue() / AstraTokenFactory.TOTAL_TOKEN_COUNT.doubleValue();
  }
}
