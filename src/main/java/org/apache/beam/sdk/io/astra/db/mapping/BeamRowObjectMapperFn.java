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

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.values.Row;
import org.joda.time.DateTime;
import org.slf4j.Logger;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

/**
 * Mapping From Cassandra Row to Beam Row.
 */
public class BeamRowObjectMapperFn implements Mapper<Row>, Serializable {

    /**
     * Cassandra Session.
     */
    private final Session session;

    /**
     * Current table.
     */
    private final String table;

    /**
     * Current keyspace.
     */
    private final String keyspace;

    /**
     * Columns of current table
     */
    private final TableMetadata tableMetadata;

    /**
     * What is the primary key
     */
    private final Set<String> primaryKeysColumnNames;

    /**
     * Constructor used by AstraDbRowMapperFactory.
     *
     * @see BeamRowObjectMapperFactory
     * @see Row
     *
     * @param session the Cassandra session.
     * @param keyspace the Cassandra keyspace to read data from.
     * @param table the Cassandra table to read from.
     */
    public BeamRowObjectMapperFn(Session session, String keyspace, String table) {
        this.session = session;
        this.keyspace = keyspace;
        this.table    = table;
        if (session.isClosed()) {
            throw new IllegalStateException("Session is already closed");
        }
        KeyspaceMetadata keyspaceMetaData = session.getCluster().getMetadata().getKeyspace("beam");
        if (keyspaceMetaData == null) {
            throw new IllegalStateException("Keyspace " + keyspace + " does not exist");
        }
        this.tableMetadata = keyspaceMetaData.getTable(table);
        if (tableMetadata == null) {
            throw new IllegalStateException("Table " + table + " does not exist");
        }
        this.primaryKeysColumnNames = tableMetadata.getPrimaryKey().stream()
                .map(ColumnMetadata::getName)
                .collect(Collectors.toSet());
    }

    @Override
    public Iterator<Row> map(ResultSet resultSet) {
        List<ColumnDefinitions.Definition> columnDefinitions =
                resultSet.getColumnDefinitions().asList();

        // Use Column MetaData to build a Schema for Beam
        Schema beamSchema = toBeamSchema(columnDefinitions);

        // Mapping Row to Row
        List<Row> rows = new ArrayList<>();
        for (com.datastax.driver.core.Row cassandraRow : resultSet) {
            List<Object> values = new ArrayList<>();
            for (int i = 0; i < columnDefinitions.size(); i++) {
                // Mapping Field to Field
                DataType cassandraColumnType = columnDefinitions.get(i).getType();
                Object columnValue = toBeamRowValue(cassandraRow.getObject(i), cassandraColumnType);
                values.add(columnValue);
            }
            Row row = Row.withSchema(beamSchema).addValues(values).build();
            rows.add(row);
        }
        return rows.iterator();
    }

    /**
     * This method takes Objects retrieved from Cassandra as well as their Column type and converts
     * them into Objects that are supported by the Beam {@link Schema} and {@link Row}.
     *
     * <p>The method starts with the complex types {@link List}, {@link Map} and {@link Set}. For all
     * other types it assumes that they are primitives and passes them on.
     *
     * @param object The Cassandra cell value to be converted.
     * @param type The Cassandra Data Type
     * @return the beam compatible object.
     */
    private Object toBeamRowValue(Object object, DataType type) {
        DataType.Name typeName = type.getName();
        if (typeName == DataType.Name.LIST) {
            DataType innerType = type.getTypeArguments().get(0);
            List list = (List) object;
            // Apply toBeamObject on all items.
            return list.stream()
                    .map(value -> toBeamObject(value, innerType))
                    .collect(Collectors.toList());
        } else if (typeName == DataType.Name.MAP) {
            DataType ktype = type.getTypeArguments().get(0);
            DataType vtype = type.getTypeArguments().get(1);
            Set<Map.Entry> map = ((Map) object).entrySet();
            // Apply toBeamObject on both key and value.
            return map.stream().collect(
                Collectors.toMap(e -> toBeamObject(e.getKey(), ktype), e -> toBeamObject(e.getValue(), vtype)));
        } else if (typeName == DataType.Name.SET) {
            DataType innerType = type.getTypeArguments().get(0);
            List list = new ArrayList((Set) object);
            // Apply toBeamObject on all items.
            return list.stream().map(l -> toBeamObject(l, innerType)).collect(Collectors.toList());
        } else {
            // scalar and default
            return toBeamObject(object, type);
        }
    }

    /**
     * Convert From Cassandra to Bean.
     *
     * @param columnDefinitions
     *      list of Column
     * @return
     */
    private Schema toBeamSchema(List<ColumnDefinitions.Definition> columnDefinitions) {
        Schema.Builder beamSchemaBuilder = Schema.builder();
        for(ColumnDefinitions.Definition colDefinition : columnDefinitions) {
            // Mapping Type including ARRAY and MAP when relevant
            Field beamField = Field.of(colDefinition.getName(), toBeamRowType(colDefinition.getType()))
                // A field is nullable if not part of primary key
                .withNullable(!primaryKeysColumnNames.contains(colDefinition.getName()))
                // As a description we can provide the original cassandra type
                .withDescription(colDefinition.getType().getName().name());
            beamSchemaBuilder.addField(beamField);
        }
        return beamSchemaBuilder.build();
    }

    /**
     * From Cassandra to Beam Type.
     */
    private FieldType toBeamRowType(DataType type) {
        DataType.Name n = type.getName();
        switch (n) {
            case TIMESTAMP:
            case DATE:
                return FieldType.DATETIME;
            case BLOB:
                return FieldType.BYTES;
            case BOOLEAN:
                return FieldType.BOOLEAN;
            case DECIMAL:
                return FieldType.DECIMAL;
            case DOUBLE:
                return FieldType.DOUBLE;
            case FLOAT:
                return FieldType.FLOAT;
            case INT:
                return FieldType.INT32;
            case VARINT:
                return FieldType.DECIMAL;
            case SMALLINT:
                return FieldType.INT16;
            case TINYINT:
                return FieldType.BYTE;
            case LIST:
            case SET:
                DataType innerType = type.getTypeArguments().get(0);
                return FieldType.array(toBeamRowType(innerType));
            case MAP:
                DataType kDataType = type.getTypeArguments().get(0);
                DataType vDataType = type.getTypeArguments().get(1);
                FieldType k = toBeamRowType(kDataType);
                FieldType v = toBeamRowType(vDataType);
                return FieldType.map(k, v);
            case VARCHAR:
            case TEXT:
            case INET:
            case UUID:
            case TIMEUUID:
            case ASCII:
                return FieldType.STRING;
            case BIGINT:
            case COUNTER:
            case TIME:
                return FieldType.INT64;
            default:
                throw new UnsupportedOperationException("Datatype " + type.getName() + " not supported.");
        }
    }

    /**
     * Most primitivies are represented the same way in Beam and Cassandra however there are a few
     * that differ. This method converts the native representation of timestamps, uuids, varint, dates
     * and IPs to a format which works for the Beam Schema.
     *
     * <p>Dates and Timestamps are returned as DateTime objects whilst UUIDs are converted to Strings.
     * Varint is converted into BigDecimal. The rest simply pass through as they are.
     *
     * @param value The object value as retrieved from Cassandra.
     * @param typeName The Cassandra schema type.
     * @see FieldType
     * @return The corresponding representation that works in the Beam schema.
     */
    private Object toBeamObject(Object value, DataType typeName) {
        if (typeName == null || typeName.getName() == null) {
            throw new UnsupportedOperationException(
                    "Unspecified Cassandra data type, cannot convert to beam row primitive.");
        }
        switch (typeName.getName()) {
            case TIMESTAMP:
                return new DateTime(value);
            case UUID:
                return ((UUID) value).toString();
            case VARINT:
                return new BigDecimal((BigInteger) value);
            case TIMEUUID:
                return ((UUID) value).toString();
            case DATE:
                LocalDate ld = (LocalDate) value;
                return new DateTime(ld.getYear(), ld.getMonth().getValue(), ld.getDayOfMonth(), 0, 0);
            case INET:
                return ((InetAddress) value).getHostAddress();
            default:
                return value;
        }
    }

    /**
     * Not used as this pipeline only reads from cassandra.
     */
    @Override
    public Future<Void> deleteAsync(Row entity) {
        // get Table Metadata to locate partition key and clustering key
        // Ensure all items of the PK are present
        // Delete with all PK element and present CC element (if any)
        // DELETE FROM table (PK,CC) VALUES (.., ..);
        throw new UnsupportedOperationException();
    }

    /**
     * Not used as this pipeline only reads from cassandra.
     */
    @Override
    public Future<Void> saveAsync(Row entity) {
        //session.executeAsync("INSERT INTO " + table);
        // get Table Metadata to locate partition key and clustering key
        // S

        // Locate Mandatory Fields to build the delete
        //
        throw new UnsupportedOperationException();
    }
}
