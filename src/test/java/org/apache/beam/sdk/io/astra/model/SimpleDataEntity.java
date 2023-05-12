package org.apache.beam.sdk.io.astra.model;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;

import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Table(name = SimpleDataEntity.TABLE_NAME)
public class SimpleDataEntity implements Serializable {

    public static final String TABLE_NAME = "simpledata";

    @PartitionKey
    protected int id;

    @Column
    protected String data;

    public SimpleDataEntity() {
    }

    public SimpleDataEntity(int id, String data) {
        this.id = id;
        this.data = data;
    }

    /**
     * Generate test data.
     *
     * @param numRows
     *      records to produce
     * @return
     *      list of records
     */
    public static List<SimpleDataEntity> generateTestData(int numRows) {
        return IntStream
                .range(0, numRows)
                .boxed()
                .map(i -> new SimpleDataEntity(i, "Record # " + i))
                .collect(Collectors.toList());
    }

    /**
     * Create Table is not exist
     *
     * @return
     *      cql statement
     */
    public static String cqlCreateTable() {
        return SchemaBuilder.createTable(TABLE_NAME)
                .addPartitionKey("id", DataType.cint())
                .addColumn("data", DataType.text())
                .ifNotExists()
                .toString();
    }

    /**
     * Cql to empty the table;
     *
     * @return
     *      cql
     */
    public static String cqlTruncateTable() {
        return "TRUNCATE TABLE " + TABLE_NAME;
    }

    @Override
    public String toString() {
        return id + ", " + data;
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
