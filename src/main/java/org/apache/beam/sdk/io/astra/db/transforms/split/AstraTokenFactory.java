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
import com.datastax.oss.driver.internal.core.metadata.token.Murmur3Token;
import com.datastax.oss.driver.internal.core.metadata.token.Murmur3TokenFactory;

import javax.annotation.Nonnull;
import java.math.BigInteger;
import java.util.Set;

/**
 * Produce Token Ranges on Mumur3 (used in AStra)
 */
public class AstraTokenFactory extends Murmur3TokenFactory {

  /** Total number of tokens in the ring. */
  public static final BigInteger TOTAL_TOKEN_COUNT =
          BigInteger.valueOf(Long.MAX_VALUE).subtract(BigInteger.valueOf(Long.MIN_VALUE));

  /**
   * Get total number of token for the ring.
   *
   * @return
   *    token number
   */
  @Nonnull
  public BigInteger totalTokenCount() {
    return TOTAL_TOKEN_COUNT;
  }

  /**
   * Compute a range of token.
   *
   * @param start
   *    start token
   * @param end
   *    end token
   * @param replicas
   *    replicas endpoints
   * @return
   *    a token range
   */
  @Nonnull
  public AstraTokenRange range(@Nonnull Token start, @Nonnull Token end, @Nonnull Set<AstraTokenRangeEndpoint> replicas) {
    return new AstraTokenRange(((Murmur3Token) start), (Murmur3Token) end, replicas);
  }

  /**
   * Create Splitter.
   *
   * @return
   *    token range splitter
   */
  @Nonnull
  public TokenRangeSplitter splitter() {
    return new AstraTokenRangeSplitter();
  }

  /**
   * Create token clusterer.
   *
   * @return
   *    clusterer.
   */
  @Nonnull
  public TokenRangeClusterer clusterer() {
    return new TokenRangeClusterer(this);
  }
}
