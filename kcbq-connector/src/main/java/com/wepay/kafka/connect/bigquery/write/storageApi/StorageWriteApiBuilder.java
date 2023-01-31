package com.wepay.kafka.connect.bigquery.write.storageApi;

import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.wepay.kafka.connect.bigquery.api.KafkaSchemaRecordType;
import com.wepay.kafka.connect.bigquery.convert.RecordConverter;
import com.wepay.kafka.connect.bigquery.convert.SchemaConverter;
import com.wepay.kafka.connect.bigquery.convert.WriteApiSchemaConverter;
import com.wepay.kafka.connect.bigquery.write.batch.TableWriterBuilder;
import org.apache.kafka.connect.sink.SinkRecord;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StorageWriteApiBuilder {

    private final TableSchema storageApiTableSchema;
    private final
    List<Object[]> records = new ArrayList<>();
    private final RecordConverter<JSONObject> storageApiRecordConverter;

    private final Schema tableSchema;

    private final TableId tableId;
    private final StorageApiStreamingBase streamWriter;
    public StorageWriteApiBuilder(StorageApiStreamingBase streamWriter,
                                  Schema tableSchema,
                                  TableId tableId,
                                  RecordConverter<JSONObject> storageApiRecordConverter) {
        this.streamWriter = streamWriter;
        SchemaConverter<TableSchema, Schema> writeApiSchemaConverter = new WriteApiSchemaConverter();
        this.tableSchema = tableSchema;
        this.tableId = tableId;
        this.storageApiTableSchema = writeApiSchemaConverter.convertSchema(tableSchema);
        this.storageApiRecordConverter = storageApiRecordConverter;
    }

    public void addRow(SinkRecord sinkRecord) {
        records.add(new Object[]{sinkRecord, convertRecord(sinkRecord)});
    }

    /**
     * Converts SinkRecord to JSONObject to be sent to BQ Streams
     * @param record which is to be converted
     * @return coverted record
     */
    private JSONObject convertRecord(SinkRecord record) {
        return storageApiRecordConverter.convertRecord(record, KafkaSchemaRecordType.VALUE);
    }


    public Runnable build() {
        return new StorageWriteApiWriter(tableSchema, tableId, streamWriter, records, storageApiTableSchema);
    }
}
