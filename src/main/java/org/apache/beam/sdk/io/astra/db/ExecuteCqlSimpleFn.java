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

/**
 * Function to execute a Fn with no Mapping.
 */
public class ExecuteCqlSimpleFn extends ExecuteCqlFn<Row> {

    /**
     * Constructor with Session.
     *
     * @param session the Cassandra session.
     */
    public ExecuteCqlSimpleFn(CqlSession session) {
        super(session);
    }

    /**
     * Constructor with read.
     *
     * @param read the Cassandra session.
     */
    public ExecuteCqlSimpleFn(AstraDbIO.Read<?> read) {
       super(read);
    }

    /**
     * Constructor with Write.
     *
     * @param write the Cassandra session.
     */
    public ExecuteCqlSimpleFn(AstraDbIO.Write<?> write) {
        super(write);
    }

    /**
     * No Mapping.
     *
     * @param row
     *      cassandra row
     * @return
     *      row
     */
    @Override
    public Row mapRow(Row row) {
        return row;
    }
}
