package org.apache.beam.sdk.io.astra.db.mapping;

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

import com.datastax.driver.core.Session;
import org.apache.beam.sdk.transforms.SerializableFunction;

/**
 * Will Convert a Cassandra Row into a Beam Row.
 */
public class BeamRowObjectMapperFactory implements SerializableFunction<Session, Mapper> {

    /**
     * Current Cassandra Keyspace.
     */
    private final String keyspace;

    /**
     * Current Cassandra Table.
     */
    private final String table;

    /**
     * Constructor.
     *
     * @param cassandraTable the Cassandra table to read from.
     * @param cassandraKeyspace the Cassandra keyspace to read from.
     */
    public BeamRowObjectMapperFactory(String cassandraKeyspace, String cassandraTable) {
        this.keyspace = cassandraKeyspace;
        this.table = cassandraTable;
    }

    @Override
    public Mapper apply(Session session) {
        return new BeamRowObjectMapperFn(session, keyspace, table);
    }
}
