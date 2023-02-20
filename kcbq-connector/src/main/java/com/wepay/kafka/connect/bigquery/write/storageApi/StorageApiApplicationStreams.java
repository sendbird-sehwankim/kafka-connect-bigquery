package com.wepay.kafka.connect.bigquery.write.storageApi;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteSettings;

public abstract class StorageApiApplicationStreams extends StorageApiStreamingBase{
    public StorageApiApplicationStreams(BigQuery bigQuery, int retry, long retryWait, BigQueryWriteSettings writeSettings) {
        super(bigQuery, retry, retryWait, writeSettings);
    }

    public abstract void createStream(String tableName);


    public abstract void finaliseStream();

    public abstract void commitStream();

    public abstract String getCurrentStreamForTable(String tableName);

}
