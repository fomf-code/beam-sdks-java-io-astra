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
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

/**
 * Specialization of PipelineOptions to work with Astra.
 */
public interface AstraDbOptions extends PipelineOptions  {

    /**
     * Access Astra Token
     * @return the Astra token
     */
    @Description("Astra Token, depending on environment could be value or secret resource id")
    @Validation.Required
    String getAstraToken();

    /**
     * Update the Astratoken
     *
     * @param token
     *      new value for AstraToken.
     */
    void setAstraToken(String token);

    /**
     * Secure Connect Bundle location. It can be;
     * - a secret resourceId
     * - a path to a file
     * - a URL
     *
     * It will be converted as a byte array for Astra DbIO
     * @return the Astra secure bundle
     */
    @Description("Location of secure connect bundle, depending on environment could be path or secret resource id")
    @Validation.Required
    String getAstraSecureConnectBundle();

    /**
     * Update the Astra secure bundle
     *
     * @param secureConnectBundleLocation
     *      new value for Astra connection timeout
     */
    @SuppressWarnings("not used")
    void setAstraSecureConnectBundle(String secureConnectBundleLocation);

    /**
     * Access Astra Keyspace
     * @return the Astra keyspace
     */
    @Description("Keyspace in Cassandra, a Db can have multiple keyspace")
    @Validation.Required
    String getAstraKeyspace();

    /**
     * Update the Astra keyspace
     *
     * @param keyspace
     *      new value for Astra keyspace
     */
    void setAstraKeyspace(String keyspace);

}
