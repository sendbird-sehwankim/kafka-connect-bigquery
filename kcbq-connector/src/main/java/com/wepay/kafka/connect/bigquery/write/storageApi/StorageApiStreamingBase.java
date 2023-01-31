package com.wepay.kafka.connect.bigquery.write.storageApi;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.Credentials;
import com.google.cloud.bigquery.*;
import com.google.cloud.bigquery.storage.v1.*;
import com.google.protobuf.Descriptors;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;

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
            Schema tableSchema,
            TableSchema recordSchema,
            List<Object[]> rows
    ) {
        Table table = this.bigQuery.getTable(tableId);
        if (table == null) {
            TableInfo tableInfo =
                    TableInfo.newBuilder(tableId, StandardTableDefinition.of(tableSchema)).build();
            this.bigQuery.create(tableInfo);
        }
        appendRows(tableName, recordSchema, rows);
    }

    abstract public void appendRows(TableName tableName, TableSchema recordSchema, List<Object[]> rows);

    public BigQueryWriteClient getWriteClient() {
        return this.writeClient;
    }
}
