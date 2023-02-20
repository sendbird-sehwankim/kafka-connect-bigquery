package com.wepay.kafka.connect.bigquery.write.storageApi;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteSettings;
import com.google.cloud.bigquery.storage.v1.TableName;
import com.google.cloud.bigquery.storage.v1.TableSchema;

import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class StorageApiPendingStream extends StorageApiApplicationStreams{

    ConcurrentMap<String, Queue<ApplicationStream>> streams ;
    public StorageApiPendingStream(BigQuery bigQuery, int retry, long retryWait, BigQueryWriteSettings writeSettings) {
        super(bigQuery, retry, retryWait, writeSettings);
        streams = new ConcurrentHashMap<>();
    }

    @Override
    public void appendRows(TableName tableName, TableSchema recordSchema, List<Object[]> rows) {

    }

    @Override
    public void createStream(String tableName) {
        synchronized(this) {
            // Create new Stream
            if(!streams.containsKey(tableName))
                this.streams.put(tableName, new LinkedList<>());
            ApplicationStream stream = new ApplicationStream(tableName);
            this.streams.get(tableName).add(stream);
        }
    }

    @Override
    public void finaliseStream() {

    }

    @Override
    public void commitStream() {

    }

    @Override
    public String getCurrentStreamForTable(String tableName) {
        if(!streams.containsKey(tableName)) {
            this.createStream(tableName);
        }
        return Objects.requireNonNull(this.streams.get(tableName).peek()).getStreamName();
    }

    public void getOffsetsReadyForCommit() {

    }
}
