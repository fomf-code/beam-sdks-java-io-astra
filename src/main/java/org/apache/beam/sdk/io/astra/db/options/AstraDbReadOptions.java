package org.apache.beam.sdk.io.astra.db.options;

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

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;

/**
 * Specialization of AstraDbOptions for Read Operations.
 */
public interface AstraDbReadOptions extends AstraDbOptions {

    /**
     * Access Astra Table name
     * @return the Astra table name
     */
    @Description("Source Table")
    String getTable();

    /**
     * Update the Astra Table
     *
     * @param table
     *      new value for Astra table name
     */
    void setTable(String table);

    /**
     * Access Astra cql query
     * @return the Astra cq query
     */
    @Description("Cql Query")
    String getQuery();

    /**
     * Update the Astra Query
     *
     * @param cqlQueryToExecute
     *      new value for Astra query to execute
     */
    void setQuery(String cqlQueryToExecute);

}
