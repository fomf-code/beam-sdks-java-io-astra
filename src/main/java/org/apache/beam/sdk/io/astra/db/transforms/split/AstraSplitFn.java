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
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.TokenMap;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import org.apache.beam.sdk.io.astra.db.AstraDbIO;
import org.apache.beam.sdk.io.astra.db.CqlSessionHolder;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.List;

/**
 * Split a Read into multiple Token Range queries.
 *
 * @param <T>
 *     current entity mapped
 */
public class AstraSplitFn<T> extends DoFn<AstraDbIO.Read<T>, AstraDbIO.Read<T>> {

    /**
     * Work with CQL and Astra.
     */
    private static final Logger LOG = LoggerFactory.getLogger(AstraSplitFn.class);

    /**
     * Default constructor.
     */
    public AstraSplitFn() {
        LOG.info("Split into Token Ranges.");
    }

    /**
     * Implement splits in token ranges.
     * @param read
     *      current read
     * @param outputReceiver
     *      splits
     */
    @ProcessElement
    public void process(@Element AstraDbIO.Read<T> read, OutputReceiver<AstraDbIO.Read<T>> outputReceiver) {
        getRingRanges(read)
                .stream()
                .map(this::mapRingRange)
                //.peek(rr -> LOG.info(rr.toString()))
                .forEach(rr -> outputReceiver.output(read.withRingRanges(ImmutableList.of(rr))));
    }

    /**
     * Sugar Synntax for mapping.
     *
     * @param astraTokenRange
     *      current range
     * @return
     *      simple serializable range
     */
    private RingRange mapRingRange(AstraTokenRange astraTokenRange) {
        return RingRange.of(
                BigInteger.valueOf(astraTokenRange.getStart().getValue()),
                BigInteger.valueOf(astraTokenRange.getEnd().getValue()));
    }

    /**
     * Compute the Ring Range.
     *
     * @param read
     *      current read
     * @param <T>
     *      entity to work with
     * @return
     *      list of token range
     */
    private static <T> List<AstraTokenRange> getRingRanges(AstraDbIO.Read<T> read) {
        Integer splitCount = evalSplitCount(read);
        LOG.info("Split query into a minimum of {} token ranges (might be more)", splitCount);
        CqlSession cqlSession  = CqlSessionHolder.getCqlSession(read);
        CqlIdentifier keyspace = cqlSession
                .getKeyspace()
                .orElseThrow(() -> new IllegalStateException("Keyspace is not available"));
        TokenMap tokenMap = cqlSession
                .getMetadata()
                .getTokenMap()
                .orElseThrow(() -> new IllegalStateException("Token map is not available"));
        return new PartitionGenerator(keyspace, tokenMap, new AstraTokenFactory()).partition(splitCount);
    }

    /**
     * Evaluate the number of splits to be created.
     * @param read
     *       current read with a read count
     * @return
     *      value for read
     */
    private static int evalSplitCount(AstraDbIO.Read<?> read) {
        if (read.minNumberOfSplits() != null && read.minNumberOfSplits().get() != null) {
            return read.minNumberOfSplits().get();
        } else {
            return CqlSessionHolder.getCqlSession(read).getMetadata().getNodes().size();
        }
    }
}
