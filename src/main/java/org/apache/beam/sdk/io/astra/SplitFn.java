package org.apache.beam.sdk.io.astra;

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

import com.datastax.driver.core.Cluster;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;

import java.math.BigInteger;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Split read per Token Range.
 *
 * @param <T>
 *     working entity
 */
public class SplitFn<T> extends DoFn<AstraIO.Read<T>, AstraIO.Read<T>> {

    /**
     * For each token ranges, update output receiver.
     * @param read
     * @param outputReceiver
     */
    @ProcessElement
    public void process(@Element AstraIO.Read<T> read, OutputReceiver<AstraIO.Read<T>> outputReceiver) {
        ((Set<RingRange>) getRingRanges(read)).stream()
                .map(ImmutableSet::of)
                .map(read::withRingRanges)
                .forEach(outputReceiver::output);
    }

    private static <T> Set<RingRange> getRingRanges(AstraIO.Read<T> read) {
        try (Cluster cluster = AstraConnectionManager.getInstance().getCluster(
                             read.token(),
                             read.consistencyLevel(),
                             read.connectTimeout(),
                             read.readTimeout(),
                             read.secureConnectBundle(),
                             read.secureConnectBundleData())) {

            Integer splitCount;
            if (read.minNumberOfSplits() != null && read.minNumberOfSplits().get() != null) {
                splitCount = read.minNumberOfSplits().get();
            } else {
                splitCount = cluster.getMetadata().getAllHosts().size();
            }
            List<BigInteger> tokens =
                    cluster.getMetadata().getTokenRanges().stream()
                            .map(tokenRange -> new BigInteger(tokenRange.getEnd().getValue().toString()))
                            .collect(Collectors.toList());
            SplitGenerator splitGenerator =
                    new SplitGenerator(cluster.getMetadata().getPartitioner());

            return splitGenerator.generateSplits(splitCount, tokens).stream()
                    .flatMap(List::stream)
                    .collect(Collectors.toSet());
        }
    }
}
