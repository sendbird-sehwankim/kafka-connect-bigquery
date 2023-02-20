package com.wepay.kafka.connect.bigquery.convert;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.wepay.kafka.connect.bigquery.constants.SchemaConstants;
import org.json.JSONObject;


import java.util.Map;
import java.util.Objects;

public class WriteApiSchemaConverter implements SchemaConverter<TableSchema, Schema> {

    /**
     * Convert a {@link Schema Cloud BigQuery Schema} into a
     * {@link com.google.cloud.bigquery.storage.v1.TableSchema BigQuery Table Schema}.
     *
     * @param schema The schema to convert.
     * @return The converted schema, which can then be used with streams.
     */

    @Override
    public TableSchema convertSchema(Schema schema) {
        TableSchema.Builder builder = TableSchema.newBuilder();
        schema.getFields().forEach(field -> builder.addFields(convertToFieldSchema(field)));

        return builder.build();
    }

    /**
     * Converts BigQuery Cloud Field Schema to BigQuery Storage Write Api Field Schema
     *
     * @param field the BigQuery cloud Field Schema
     * @return the BigQuery Write Api Field Schema
     */
    public TableFieldSchema convertToFieldSchema(Field field) {
        TableFieldSchema.Builder fieldBuilder = TableFieldSchema.newBuilder();
        // Though `setMode` is mentioned as optional, not setting this field gives runtime error
        fieldBuilder.setMode(Objects.requireNonNull(
                SchemaConstants.BQTableSchemaModeMap.getOrDefault(field.getMode(), TableFieldSchema.Mode.NULLABLE)));

        fieldBuilder.setName(field.getName());
        fieldBuilder.setType(Objects.requireNonNull(SchemaConstants.BQTableSchemaTypeMap.get(field.getType().getStandardType())));
        if (field.getDescription() != null) {
            fieldBuilder.setDescription(field.getDescription());
        }
        if (field.getSubFields() != null) {
            field.getSubFields().forEach(f -> fieldBuilder.addFields(convertToFieldSchema(f)));
        }

        return fieldBuilder.build();
    }

    public TableFieldSchema convertMapEntrytoFieldSchema(String key, Object value) {
        TableFieldSchema.Builder fieldBuilder = TableFieldSchema.newBuilder();
        // Though `setMode` is mentioned as optional, not setting this field gives runtime error
        fieldBuilder.setMode(TableFieldSchema.Mode.NULLABLE);

        fieldBuilder.setName(key);
        if(value instanceof String) {
            fieldBuilder.setType(TableFieldSchema.Type.STRING);
        } else if(value instanceof  Boolean) {
            fieldBuilder.setType(TableFieldSchema.Type.BOOL);
        } else if(value instanceof Byte) {
            fieldBuilder.setType(TableFieldSchema.Type.BYTES);
        } else if(value instanceof Integer) {
            fieldBuilder.setType(TableFieldSchema.Type.INT64);
        } else if(value instanceof Long || value instanceof Float) {
            fieldBuilder.setType(TableFieldSchema.Type.DOUBLE);
        } else if(value instanceof Map) {
            ((Map<String, Object>) value).forEach((k,v) -> fieldBuilder.addFields(convertMapEntrytoFieldSchema(k,v)));
        }

        return fieldBuilder.build();
    }

    public TableSchema convertSchemaless(JSONObject record) {
        TableSchema.Builder builder = TableSchema.newBuilder();
        record.toMap().forEach((k,v) -> builder.addFields(convertMapEntrytoFieldSchema(k,v)));

        return builder.build();
    }
}
