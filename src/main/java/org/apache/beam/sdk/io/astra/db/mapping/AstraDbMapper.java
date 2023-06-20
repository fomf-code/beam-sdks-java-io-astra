/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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

import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.mapper.annotations.Delete;
import com.datastax.oss.driver.api.mapper.annotations.GetEntity;
import com.datastax.oss.driver.api.mapper.annotations.Insert;

import java.util.Iterator;
import java.util.concurrent.CompletionStage;

/**
 * Map Cassandra entities to Java objects.
 */
public interface AstraDbMapper<T> {

  @GetEntity
  T mapRow(Row row);

  /**
   * This method is called when reading data from Cassandra. It should map a ResultSet into the
   * corresponding Java objects.
   *
   * @param resultSet A resultset containing rows.
   * @return An iterator containing the objects that you want to provide to your downstream
   *     pipeline.
   */
  default Iterator<T> map(ResultSet resultSet) {
    return resultSet.map(this::mapRow).iterator();
  }

  /**
   * This method is called for each delete event. The input argument is the Object that should be
   * deleted in Cassandra. The return value should be a Future that completes when the delete action
   * is completed.
   *
   * @param entity
   *    Entity to be deleted.
   * @return
   *    Future when complete.
   */
  @Delete
  CompletionStage<Void> deleteAsync(T entity);

  /**
   * This method is called for each save event. The input argument is the Object that should be
   * saved or updated in Cassandra. The return value should be a future that completes when the save
   * action is completed.
   *
   * @param entity
   *    Entity to be saved.
   * @return
   *    Future when complete.
   */
  @Insert
  CompletionStage<Void> saveAsync(T entity);

}
