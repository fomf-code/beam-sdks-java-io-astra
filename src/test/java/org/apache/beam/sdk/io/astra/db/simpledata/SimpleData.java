package org.apache.beam.sdk.io.astra.db.simpledata;

import java.io.Serializable;

public class SimpleData implements Serializable {

    public static final String TABLE_NAME = "simpledata";

    int id;

    String data;

    @Override
    public String toString() {
        return id + ", " + data;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        SimpleData that = (SimpleData) o;
        return id == that.id && data.equals(that.data);
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(id, data);
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
     * Gets data
     *
     * @return value of data
     */
    public String getData() {
        return data;
    }

    /**
     * Set value for data
     *
     * @param data
     *         new value for data
     */
    public void setData(String data) {
        this.data = data;
    }
}