package org.apache.beam.sdk.io.astra.db.scientist;

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
import org.apache.beam.sdk.io.astra.db.mapping.AstraDbMapper;
import org.apache.beam.sdk.transforms.SerializableFunction;

public class ScientistMapperFactoryFn implements SerializableFunction<CqlSession, AstraDbMapper<Scientist>> {

    @Override
    public ScientistDao apply(CqlSession cqlSession) {
        return new ScientistDaoMapperBuilder(cqlSession).build()
                .getScientistDao(cqlSession.getKeyspace().get());
    }

}
