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
import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/** Splits token ranges into smaller sub-ranges. */
public interface TokenRangeSplitter {

  /**
   * Splits the given ranges uniformly into (smaller) {@code splitCount} chunks.
   *
   * <p>Note that the algorithm is size-based and doesn't guarantee exact split count.
   *
   * @param tokenRanges The ranges to split.
   * @param splitCount The desired number of resulting chunks.
   * @return A list of ranges of approximately {@code splitCount} chunks.
   */
  @Nonnull
  default List<AstraTokenRange> split(@Nonnull Iterable<AstraTokenRange> tokenRanges, int splitCount) {
    double ringFractionPerSplit = 1.0 / (double) splitCount;
    return StreamSupport.stream(tokenRanges.spliterator(), false)
        .flatMap(
            range -> {
              int splits = (int) Math.max(1, Math.rint(range.fraction() / ringFractionPerSplit));
              List<AstraTokenRange> split = splits == 1 ? Collections.singletonList(range) : split(range, splits);
              return split.stream();
            })
        .collect(Collectors.toList());
  }

  /**
   * Splits the given token range uniformly into sub-ranges with the given desired split count.
   *
   * <p>Note that the algorithm is size-based and doesn't guarantee exact split count.
   *
   * @param tokenRange The range to split.
   * @param splitCount The desired number of resulting chunks.
   * @return A list of ranges of approximately {@code splitCount} chunks.
   */
  @Nonnull
  List<AstraTokenRange> split(@Nonnull AstraTokenRange tokenRange, int splitCount);
}
