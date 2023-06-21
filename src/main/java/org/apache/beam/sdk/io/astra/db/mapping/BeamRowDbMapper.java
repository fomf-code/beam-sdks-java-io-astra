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

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.CqlVectorType;
import com.datastax.oss.driver.api.core.type.CustomType;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.ListType;
import com.datastax.oss.driver.api.core.type.MapType;
import com.datastax.oss.driver.api.core.type.SetType;
import com.datastax.oss.driver.api.core.type.TupleType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.values.Row;
import org.joda.time.DateTime;

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
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Mapping From Cassandra Row to Beam Row.
 */
public class BeamRowDbMapper implements AstraDbMapper<Row>, Serializable {

    /**
     * Cassandra Session.
     */
    private final CqlSession session;

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

    List<ColumnDefinition> columnDefinitions;

    Schema beamSchema;

    /**
     * Constructor used by AstraDbRowMapperFactory.
     *
     * @see BeamRowDbMapperFactoryFn
     * @see Row
     *
     * @param session the Cassandra session.
     * @param keyspace the Cassandra keyspace to read data from.
     * @param table the Cassandra table to read from.
     */
    public BeamRowDbMapper(CqlSession session, String keyspace, String table) {
        this.session = session;
        this.keyspace = keyspace;
        this.table    = table;
        if (session.isClosed()) {
            throw new IllegalStateException("Session is already closed");
        }

        KeyspaceMetadata keyspaceMetaData = session
                .getMetadata().getKeyspace(keyspace)
                .orElseThrow(()-> new IllegalStateException("Keyspace "
                        + keyspace + " does not exist"));

        this.tableMetadata = keyspaceMetaData
                .getTable(table)
                .orElseThrow(() -> new IllegalStateException("Table "
                        + table + " does not exist for keyspace "
                        + keyspace + ""));

        this.primaryKeysColumnNames = tableMetadata.getPartitionKey().stream()
                .map(ColumnMetadata::getName)
                .map(id -> id.asCql(true))
                .collect(Collectors.toSet());

    }

    @Override
    public Row mapRow(com.datastax.oss.driver.api.core.cql.Row cassandraRow) {
        List<Object> values = new ArrayList<>();
        for (int i = 0; i < columnDefinitions.size(); i++) {
            // Mapping Field to Field
            DataType cassandraColumnType = columnDefinitions.get(i).getType();
            Object columnValue = toBeamRowValue(cassandraRow.getObject(i), cassandraColumnType);
            values.add(columnValue);
        }
        return Row.withSchema(beamSchema).addValues(values).build();
    }

    @Override
    public Iterator<Row> map(ResultSet resultSet) {
       columnDefinitions = StreamSupport
                .stream(resultSet.getColumnDefinitions().spliterator(), false)
                .collect(Collectors.toList());
        beamSchema = toBeamSchema(columnDefinitions);
        List<Row> rows = new ArrayList<>();
        for (com.datastax.oss.driver.api.core.cql.Row cassandraRow : resultSet) {
            rows.add(mapRow(cassandraRow));
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
        if (type instanceof ListType) {
            // Process LISTS
            ListType listType = (ListType) type;
            DataType innerType = listType.getElementType();
            List list = (List) object;
            return list.stream()
                    .map(value -> toBeamObject(value, innerType))
                    .collect(Collectors.toList());
        } else if (type instanceof SetType) {
            // Process with SETS
            SetType setType = (SetType) type;
            DataType innerType = setType.getElementType();
            Set set = (Set) object;
            return set.stream()
                    .map(value -> toBeamObject(value, innerType))
                    .collect(Collectors.toSet());
        } else if (type instanceof MapType) {
            // Processing with MAPS
            MapType mapType = (MapType) type;
            DataType kType = mapType.getKeyType();
            DataType vType = mapType.getValueType();
            Set<Map.Entry> map = ((Map) object).entrySet();
            // Apply toBeamObject on both key and value.
            return map.stream().collect(
                    Collectors.toMap(e -> toBeamObject(e.getKey(), kType), e -> toBeamObject(e.getValue(), vType)));
        } else if (type instanceof TupleType) {
            throw new IllegalArgumentException("As of today there is no support of Tuple in Beam");
        } else if (type instanceof UserDefinedType) {
            throw new IllegalArgumentException("As of today there is no support of Custom Format in Beam");
        } else if (type instanceof CqlVectorType) {

        }
        return toBeamObject(object, type);
    }

    /**
     * Convert From Cassandra to Bean.
     *
     * @param columnDefinitions
     *      list of Column
     * @return
     */
    private Schema toBeamSchema(List<ColumnDefinition> columnDefinitions) {
        Schema.Builder beamSchemaBuilder = Schema.builder();
        for(ColumnDefinition colDefinition : columnDefinitions) {
            // Mapping Type including ARRAY and MAP when relevant
            Field beamField = Field.of(colDefinition.getName().toString(), toBeamRowType(colDefinition.getType()))
                // A field is nullable if not part of primary key
                .withNullable(!primaryKeysColumnNames.contains(colDefinition.getName()))
                // As a description we can provide the original cassandra type
                .withDescription(String.valueOf(colDefinition.getType().getProtocolCode()));
            beamSchemaBuilder.addField(beamField);
        }
        return beamSchemaBuilder.build();
    }

    /**
     * From Cassandra to Beam Type.
     */
    private FieldType toBeamRowType(DataType type) {
        switch (type.getProtocolCode()) {

            case ProtocolConstants.DataType.UUID:
            case ProtocolConstants.DataType.VARCHAR:
            case ProtocolConstants.DataType.ASCII:
            case ProtocolConstants.DataType.TIMEUUID:
            case ProtocolConstants.DataType.INET:
                return FieldType.STRING;

            case ProtocolConstants.DataType.VARINT:
            case ProtocolConstants.DataType.DECIMAL:
                return FieldType.DECIMAL;

            case ProtocolConstants.DataType.COUNTER:
            case ProtocolConstants.DataType.BIGINT:
            case ProtocolConstants.DataType.TIME:
                return FieldType.INT64;

            case ProtocolConstants.DataType.DATE:
            case ProtocolConstants.DataType.TIMESTAMP:
                return FieldType.DATETIME;

            case ProtocolConstants.DataType.DOUBLE:
                return FieldType.DOUBLE;
            case ProtocolConstants.DataType.FLOAT:
                return FieldType.FLOAT;
            case ProtocolConstants.DataType.INT:
            case ProtocolConstants.DataType.DURATION:
                return FieldType.INT32;
            case ProtocolConstants.DataType.SMALLINT:
                return FieldType.INT16;
            case ProtocolConstants.DataType.TINYINT:
                return FieldType.BYTE;
            case ProtocolConstants.DataType.BLOB:
                return FieldType.BYTES;
            case ProtocolConstants.DataType.BOOLEAN:
                return FieldType.BOOLEAN;
            case ProtocolConstants.DataType.CUSTOM:
                CustomType ct = (CustomType) type;
                if (type instanceof CqlVectorType) {
                    return FieldType.BYTES;
                }
            default:
                throw new IllegalArgumentException("Cannot Map Cassandra Type " + type.getProtocolCode() + " to Beam Type");
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
        switch (typeName.getProtocolCode()) {
            case ProtocolConstants.DataType.TIMESTAMP:
                return new DateTime(value);
            case ProtocolConstants.DataType.UUID:
                return ((UUID) value).toString();
            case ProtocolConstants.DataType.VARINT:
                return new BigDecimal((BigInteger) value);
            case ProtocolConstants.DataType.TIMEUUID:
                return value.toString();
            case ProtocolConstants.DataType.DATE:
                LocalDate ld = (LocalDate) value;
                return new DateTime(ld.getYear(), ld.getMonth().getValue(), ld.getDayOfMonth(), 0, 0);
            case ProtocolConstants.DataType.INET:
                return ((InetAddress) value).getHostAddress();
            default:
                return value;
        }
    }

    /**
     * Not used as this pipeline only reads from cassandra.
     */
    @Override
    public CompletionStage<Void> deleteAsync(Row entity) {
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
    public CompletionStage<Void> saveAsync(Row entity) {
        //session.executeAsync("INSERT INTO " + table);
        // get Table Metadata to locate partition key and clustering key
        // S

        // Locate Mandatory Fields to build the delete
        //
        throw new UnsupportedOperationException();
    }
}
