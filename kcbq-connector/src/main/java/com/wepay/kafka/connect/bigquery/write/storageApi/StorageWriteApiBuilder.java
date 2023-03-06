package com.wepay.kafka.connect.bigquery.write.storageApi;

import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.wepay.kafka.connect.bigquery.api.KafkaSchemaRecordType;
import com.wepay.kafka.connect.bigquery.api.SchemaRetriever;
import com.wepay.kafka.connect.bigquery.convert.RecordConverter;
import com.wepay.kafka.connect.bigquery.convert.SchemaConverter;
import com.wepay.kafka.connect.bigquery.convert.WriteApiSchemaConverter;
import com.wepay.kafka.connect.bigquery.utils.TableNameUtils;
import com.wepay.kafka.connect.bigquery.write.batch.TableWriterBuilder;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StorageWriteApiBuilder {

    //private final TableSchema storageApiTableSchema;
    private final
    List<Object[]> records = new ArrayList<>();
    private final RecordConverter<JSONObject> storageApiRecordConverter;

    private final SchemaRetriever schemaRetriever;
    private final TableId tableId;
    private final StorageApiStreamingBase streamWriter;
    private final StorageApiBatchModeHandler batchModeHandler;

    public StorageWriteApiBuilder(StorageApiStreamingBase streamWriter,
                                  SchemaRetriever schemaRetriever,
                                  TableId tableId,
                                  RecordConverter<JSONObject> storageApiRecordConverter,
                                  StorageApiBatchModeHandler batchModeHandler) {
        this.streamWriter = streamWriter;
        SchemaConverter<TableSchema, Schema> writeApiSchemaConverter = new WriteApiSchemaConverter();
        this.tableId = tableId;
        this.schemaRetriever = schemaRetriever;
        //this.storageApiTableSchema = writeApiSchemaConverter.convertSchema(tableSchema);
        this.storageApiRecordConverter = storageApiRecordConverter;
        this.batchModeHandler = batchModeHandler;
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


    private String updateAndGetStream(){
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        records.forEach(record ->  {
            SinkRecord sr = (SinkRecord) record[0];
            offsets.put(new TopicPartition(sr.topic(),sr.kafkaPartition()), new OffsetAndMetadata(sr.kafkaOffset()+1) );
        }  );
        String streamName =  batchModeHandler.getCurrentStream(TableNameUtils.tableName(tableId).toString());
        batchModeHandler.updateOffsetsForStream(TableNameUtils.tableName(tableId).toString(),streamName, offsets);
        return streamName;
    }
    public Runnable build() {
        String streamName = null;
        if(streamWriter instanceof StorageApiPendingStream) {
            streamName = updateAndGetStream();
        }
        return new StorageWriteApiWriter(tableId, streamWriter, records, schemaRetriever, streamName);
    }
}
