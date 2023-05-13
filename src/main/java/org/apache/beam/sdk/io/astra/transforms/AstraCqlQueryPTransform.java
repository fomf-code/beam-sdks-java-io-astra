package org.apache.beam.sdk.io.astra.transforms;

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

import com.datastax.driver.core.ConsistencyLevel;
import org.apache.beam.sdk.io.astra.ConnectionManager;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * Execute CQL Against Astra.
 *
 * @param <T>
 *      current bean
 */
public class AstraCqlQueryPTransform<T> extends PTransform<PCollection<T>, PCollection<T>> {

    /**
     * Logger
     */
    private static final Logger LOG = LoggerFactory.getLogger(ConnectionManager.class);

    /**
     * Execute a CQL query
     *
     * @param token
     * @param secureConnectBundle
     * @param keyspace
     */
    public AstraCqlQueryPTransform(String token, File secureConnectBundle, String keyspace, String cql) {
        LOG.info("Executing CQL: {}", cql);
        ConnectionManager
                .getInstance()
                .getSession(
                        ValueProvider.StaticValueProvider.of(token),
                        ValueProvider.StaticValueProvider.of(ConsistencyLevel.LOCAL_QUORUM.name()),
                        ValueProvider.StaticValueProvider.of(20000),
                        ValueProvider.StaticValueProvider.of(20000),
                        ValueProvider.StaticValueProvider.of(secureConnectBundle),
                        null,
                        keyspace)
                .execute(cql);
    }

    /**
     * Execute a CQL query
     *
     * @param token
     * @param secureConnectBundle
     * @param keyspace
     */
    public AstraCqlQueryPTransform(String token, byte[] secureConnectBundle, String keyspace, String cql) {
        LOG.info("Executing CQL: {}", cql);
        ConnectionManager
                .getInstance()
                .getSession(
                        ValueProvider.StaticValueProvider.of(token),
                        ValueProvider.StaticValueProvider.of(ConsistencyLevel.LOCAL_QUORUM.name()),
                        ValueProvider.StaticValueProvider.of(20000),
                        ValueProvider.StaticValueProvider.of(20000),
                        null,
                        ValueProvider.StaticValueProvider.of(secureConnectBundle), keyspace)
                .execute(cql);
    }

    @Override
    public PCollection<T> expand(PCollection<T> input) {
        return input;
    }

}
