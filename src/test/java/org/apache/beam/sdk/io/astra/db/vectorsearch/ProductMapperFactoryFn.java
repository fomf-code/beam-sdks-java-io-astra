//package org.apache.beam.sdk.io.astra.db.vectorsearch;
//
///*-
// * #%L
// * Beam SDK for Astra
// * --
// * Copyright (C) 2023 DataStax
// * --
// * Licensed under the Apache License, Version 2.0
// * You may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// *   http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// * #L%
// */
//
//import com.datastax.oss.driver.api.core.CqlSession;
//import com.datastax.oss.driver.api.core.cql.Row;
//import org.apache.beam.sdk.io.astra.db.mapping.AstraDbMapper;
//import org.apache.beam.sdk.transforms.SerializableFunction;
//
//import java.util.concurrent.CompletionStage;
//
//public class ProductMapperFactoryFn implements SerializableFunction<CqlSession, AstraDbMapper<Product>> {
//
//    @Override
//    public ProductDao apply(CqlSession cqlSession) {
//        return new ProductDaoMapperBuilder(cqlSession).build()
//                .getProductDao(cqlSession.getKeyspace().get());
//    }
//
//}
