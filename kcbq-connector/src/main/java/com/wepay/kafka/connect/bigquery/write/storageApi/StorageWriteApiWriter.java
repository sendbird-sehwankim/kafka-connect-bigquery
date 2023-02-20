package com.wepay.kafka.connect.bigquery.write.storageApi;

import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.storage.v1.TableName;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.wepay.kafka.connect.bigquery.api.SchemaRetriever;

import java.util.List;

public class StorageWriteApiWriter implements Runnable {

    private final StorageApiStreamingBase streamWriter;

   // private final Schema tableSchema;

    private final TableId tableId;
    private final List<Object[]> records;

    private final SchemaRetriever schemaRetriever;
    public StorageWriteApiWriter(TableId tableId, StorageApiStreamingBase streamWriter, List<Object[]> records, SchemaRetriever schemaRetriever) {

        this.schemaRetriever = schemaRetriever;
        this.streamWriter = streamWriter;
        this.records = records;
        //this.streamSchema = schema;
        this.tableId = tableId;
       // this.tableSchema = tableSchema;
    }

    @Override
    public void run() {
        TableName tableName = getTableNameFromId(tableId);
        streamWriter.initializeAndWriteRecords(tableName, tableId, records, schemaRetriever);

    }

    private TableName getTableNameFromId(TableId id) {
        return TableName.of(id.getProject(), id.getDataset(), id.getTable());
    }
}
