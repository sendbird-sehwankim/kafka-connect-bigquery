package com.wepay.kafka.connect.bigquery.convert;

import com.wepay.kafka.connect.bigquery.api.KafkaSchemaRecordType;
import com.wepay.kafka.connect.bigquery.exception.ConversionConnectException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.json.JSONObject;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;

public class WriteApiRecordConverter implements RecordConverter<JSONObject>{

    private static final Set<Class<?>> BASIC_TYPES = new HashSet<>(
            Arrays.asList(
                    Boolean.class, Character.class, Byte.class, Short.class,
                    Integer.class, Long.class, Float.class, Double.class, String.class)
    );
    /**
     * Converts a {@link SinkRecord} to {@link JSONObject} for Storage write api JsonStream
     * @param record The record to convert.
     * @param recordType The type of the record to convert, either value or key.
     * @return JSONObject
     */
    @Override
    public JSONObject convertRecord(SinkRecord record, KafkaSchemaRecordType recordType) {
        Schema recordSchema = recordType == KafkaSchemaRecordType.KEY? record.keySchema() : record.valueSchema();
        Object data = recordType == KafkaSchemaRecordType.KEY? record.key() : record.value();

        if(recordSchema == null) {
            //TODO: this needs to be done
            if (data instanceof Map) {
                return (JSONObject) convertSchemalessRecord(data);
            }
            throw new ConversionConnectException("Only Map objects supported in absence of schema for " +
                    "record conversion to BigQuery format.");
        }
        if(recordSchema.type() == Schema.Type.STRUCT) {
            return convertStructToJSON(recordSchema, (Struct) data);
        } else {
            //TODO: this needs to be done
            return new JSONObject();
        }

    }

    private Object convertSchemalessRecord(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Double) {
            return value;
        }
        if (BASIC_TYPES.contains(value.getClass())) {
            return value;
        }
        if (value instanceof byte[] || value instanceof ByteBuffer) {
            return convertBytes(value);
        }
        if (value instanceof List) {
            return ((List<?>) value).stream()
                    .map(this::convertSchemalessRecord)
                    .collect(Collectors.toList());
        }
        if (value instanceof Map) {
            JSONObject result = new JSONObject();
            ((Map<Object, Object>) value).forEach((k,v) -> {
                if (!(k instanceof String)) {
                    throw new ConversionConnectException(
                            "Failed to convert record to bigQuery format: " +
                                    "Map objects in absence of schema needs to have string value keys. ");
                }
                result.put((String)k, convertSchemalessRecord(v));
            });
            return result;
        }
        throw new ConversionConnectException("Unsupported class " + value.getClass() +
                " found in schemaless record data. Can't convert record to bigQuery format");
    }
    private Object convertToObject(Schema recordSchema, Object recordContent) {
        if(recordContent == null) {
            return recordSchema.isOptional() ? null: null; //TODO:: throw exception
        }
        switch(recordSchema.type()) {
            case STRUCT: return convertStructToJSON(recordSchema, (Struct) recordContent);
            case INT8:
            case STRING:
            case INT32:
            case INT64:
            case INT16:
            case FLOAT32:
            case FLOAT64:
            case BOOLEAN:
                return recordContent;
        }
        return recordContent;
    }
    private JSONObject convertStructToJSON(Schema recordSchema, Struct recordContent) {
        JSONObject result = new JSONObject();
        recordSchema.fields().forEach(field -> {
            Object obj  = convertToObject(field.schema(), recordContent.get(field.name()));
            if(obj != null) {
                result.put(field.name(), obj);
            }
        });

        return result;
    }

    private String convertBytes(Object kafkaConnectObject) {
        byte[] bytes;
        if (kafkaConnectObject instanceof ByteBuffer) {
            ByteBuffer byteBuffer = (ByteBuffer) kafkaConnectObject;
            bytes = byteBuffer.array();
        } else {
            bytes = (byte[]) kafkaConnectObject;
        }
        return Base64.getEncoder().encodeToString(bytes);
    }
}
