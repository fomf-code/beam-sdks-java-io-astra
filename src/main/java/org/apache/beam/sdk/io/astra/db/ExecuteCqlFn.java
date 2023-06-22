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

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import org.apache.beam.sdk.transforms.DoFn;

/**
 * DoFn to execute a Query
 */
public abstract class ExecuteCqlFn<T> extends DoFn<String, Row> {

    /**
     * The Cassandra session.
     */
    CqlSession cqlSession;

    /**
     * Constructor with Session.
     *
     * @param session the Cassandra session.
     */
    public ExecuteCqlFn(CqlSession session) {
        this.cqlSession = session;
    }

    /**
     * Constructor with read.
     *
     * @param read reader.
     */
    public ExecuteCqlFn(AstraDbIO.Read<?> read) {
        this.cqlSession = CqlSessionHolder.getCqlSession(
                read.token(), read.secureConnectBundle(), read.keyspace());
    }

    /**
     * Constructor with write.
     *
     * @param write writer
     */
    public ExecuteCqlFn(AstraDbIO.Write<?> write) {
        this.cqlSession = CqlSessionHolder.getCqlSession(
                write.token(), write.secureConnectBundle(), write.keyspace());
    }

    /**
     * Executing query as implementation of the DoFn.
     * @param query
     *      current query
     * @param receiver
     *      query responses
     */
    @ProcessElement
    public void processElement(@Element String query, OutputReceiver<T> receiver) {
        cqlSession.execute(query).forEach(row -> receiver.output(mapRow(row)));
    }

    /**
     * Show be implemented to map row.
     *
     * @param row
     *      target row
     * @return
     *      row mapper
     */
    public abstract T mapRow(Row row);


}
