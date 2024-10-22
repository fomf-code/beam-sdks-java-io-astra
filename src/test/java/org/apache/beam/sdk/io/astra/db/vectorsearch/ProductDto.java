package org.apache.beam.sdk.io.astra.db.vectorsearch;

import com.datastax.oss.driver.api.core.data.CqlVector;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class ProductDto implements Serializable {

    private int id;

    private String name;

    private String description;

    private List<Float> vector;

    public ProductDto() {}

    public ProductDto(Product p) {
        this.id   = p.getId();
        this.name = p.getName();
        this.description = p.getDescription();
        this.vector = StreamSupport
                .stream(p.getVector().spliterator(), false)
                .collect(Collectors.toList());
    }

    public Product toProduct() {
        Product p = new Product();
        p.setId(this.id);
        p.setName(this.name);
        p.setDescription(this.description);
        p.setVector(CqlVector.newInstance(this.vector));
        return p;
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
    public List<Float> getVector() {
        return vector;
    }

    /**
     * Set value for vector
     *
     * @param vector
     *         new value for vector
     */
    public void setVector(List<Float> vector) {
        this.vector = vector;
    }
}
