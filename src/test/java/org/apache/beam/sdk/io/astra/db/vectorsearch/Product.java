package org.apache.beam.sdk.io.astra.db.vectorsearch;

import com.datastax.oss.driver.api.core.data.CqlVector;
import com.datastax.oss.driver.api.mapper.annotations.CqlName;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;

import java.io.Serializable;

@Entity
@CqlName(Product.TABLE_NAME)
public class Product implements Serializable {

    /** Table Name. */
    public static final String TABLE_NAME = "products";

    @PartitionKey
    @CqlName("id")
    private int id;

    @CqlName("name")
    private String name;

    @CqlName("description")
    private String description;

    @CqlName("item_vector")
    private CqlVector<Float> vector;

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

    /**
     * Gets description
     *
     * @return value of description
     */
    public String getDescription() {
        return description;
    }

    /**
     * Set value for description
     *
     * @param description
     *         new value for description
     */
    public void setDescription(String description) {
        this.description = description;
    }

    /**
     * Gets vector
     *
     * @return value of vector
     */
    public CqlVector<Float> getVector() {
        return vector;
    }

    /**
     * Set value for vector
     *
     * @param vector
     *         new value for vector
     */
    public void setVector(CqlVector<Float> vector) {
        this.vector = vector;
    }

}
