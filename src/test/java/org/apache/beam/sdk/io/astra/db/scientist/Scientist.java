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

import com.datastax.oss.driver.api.mapper.annotations.ClusteringColumn;
import com.datastax.oss.driver.api.mapper.annotations.CqlName;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Objects;

import java.io.Serializable;

/**
 * CREATE TABLE IF NOT EXISTS %s.%s(
 *   person_department text,
 *   person_id int,
 *   person_name text,
 *   PRIMARY KEY((person_department), person_id)
 * );"
 */
@Entity
@CqlName(Scientist.TABLE_NAME)
public class Scientist implements Serializable {

    public static final String TABLE_NAME = "scientist";

    @PartitionKey
    @CqlName("person_department")
    private String department;

    @ClusteringColumn
    @CqlName("person_id")
    private int id;

    @CqlName("person_name")
    private String name;

    public Scientist() {
    }

    public Scientist(String personDepartment, int personId, String personName) {
        this.department = personDepartment;
        this.id = personId;
        this.name = personName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Scientist scientist = (Scientist) o;
        return id == scientist.id
                && Objects.equal(name, scientist.name)
                && Objects.equal(department, scientist.department);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(name, id);
    }

    /**
     * Gets department
     *
     * @return value of department
     */
    public String getDepartment() {
        return department;
    }

    /**
     * Set value for department
     *
     * @param department
     *         new value for department
     */
    public void setDepartment(String department) {
        this.department = department;
    }

    /**
     * Gets id
     *
     * @return value of id
     */
    public int getId() {
        return id;
    }

    /**
     * Set value for id
     *
     * @param id
     *         new value for id
     */
    public void setId(int id) {
        this.id = id;
    }

    /**
     * Gets name
     *
     * @return value of name
     */
    public String getName() {
        return name;
    }

    /**
     * Set value for name
     *
     * @param name
     *         new value for name
     */
    public void setName(String name) {
        this.name = name;
    }
}
