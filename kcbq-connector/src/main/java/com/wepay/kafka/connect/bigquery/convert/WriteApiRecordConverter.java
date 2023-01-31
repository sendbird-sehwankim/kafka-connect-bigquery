package com.wepay.kafka.connect.bigquery.convert;

import com.wepay.kafka.connect.bigquery.api.KafkaSchemaRecordType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.json.JSONObject;

public class WriteApiRecordConverter implements RecordConverter<JSONObject>{

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
            return new JSONObject();
        }
        if(recordSchema.type() == Schema.Type.STRUCT) {
            return convertStructToJSON(recordSchema, (Struct) data);
        } else {
            //TODO: this needs to be done
            return new JSONObject();
        }

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
}
