package com.wepay.kafka.connect.bigquery.write.storageApi;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.Credentials;
import com.google.cloud.bigquery.*;
import com.google.cloud.bigquery.storage.v1.*;
import com.google.protobuf.Descriptors;
import com.wepay.kafka.connect.bigquery.api.SchemaRetriever;
import com.wepay.kafka.connect.bigquery.convert.BigQuerySchemaConverter;
import com.wepay.kafka.connect.bigquery.convert.WriteApiSchemaConverter;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.json.JSONObject;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public abstract class StorageApiStreamingBase {

    private final BigQueryWriteClient writeClient;

    private final int retry;

    private final long retryWait;

    private final BigQuery bigQuery;

    public StorageApiStreamingBase(BigQuery bigQuery, int retry, long retryWait, BigQueryWriteSettings writeSettings) {
        this.bigQuery = bigQuery;
        this.retry = retry;
        this.retryWait = retryWait;
        try {
            this.writeClient = BigQueryWriteClient.create(writeSettings);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    public void initializeAndWriteRecords(
            TableName tableName,
            TableId tableId,
            List<Object[]> rows,
            SchemaRetriever schemaRetriever
    ) {

        Schema tableSchema = null;
        SinkRecord initialRecord = (SinkRecord) rows.get(0)[0];
        org.apache.kafka.connect.data.Schema valueSchema = schemaRetriever.retrieveValueSchema(initialRecord);
        TableSchema recordSchema;
        if(valueSchema != null) {
            tableSchema = new BigQuerySchemaConverter(false, true)
                    .convertSchema(valueSchema);
            recordSchema = new WriteApiSchemaConverter().convertSchema(tableSchema);
        } else {
            recordSchema = null;
        }

        // fails without lock, fix by locking
        if (tableSchema != null) {
            Table table = this.bigQuery.getTable(tableId);
            if (table == null) {
                synchronized (this) {
                    if (this.bigQuery.getTable(tableId) == null) {
                        TableInfo tableInfo =
                                TableInfo.newBuilder(tableId, StandardTableDefinition.of(tableSchema)).build();
                        try {
                            this.bigQuery.create(tableInfo);
                        } catch (BigQueryException exception) {
                            // if already exists, move on
                        }

                    }
                }
            }
        }
        appendRows(tableName, recordSchema, rows);
    }

    abstract public void appendRows(TableName tableName, TableSchema recordSchema, List<Object[]> rows);

    public BigQueryWriteClient getWriteClient() {
        return this.writeClient;
    }
}
