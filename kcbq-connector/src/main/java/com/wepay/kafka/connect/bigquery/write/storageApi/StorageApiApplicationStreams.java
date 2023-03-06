package com.wepay.kafka.connect.bigquery.write.storageApi;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteSettings;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public abstract class StorageApiApplicationStreams extends StorageApiStreamingBase{

    Logger logger = LoggerFactory.getLogger(StorageApiApplicationStreams.class);
    public StorageApiApplicationStreams(BigQuery bigQuery, int retry, long retryWait, BigQueryWriteSettings writeSettings) {
        super(bigQuery, retry, retryWait, writeSettings);
    }

    public abstract void createStream(String tableName);

    public abstract String getCurrentStreamForTable(String tableName);

    public abstract Map<TopicPartition, OffsetAndMetadata> getCommitableOffsets();

    public void updateOffsetsForStream(String tableName,String streamName,  Map<TopicPartition, OffsetAndMetadata> offsetInfo) {
        String msg = String.format("Stream %s does not support updating offsets on table %s", streamName, tableName);
        logger.error(msg);
        throw new RuntimeException(msg);
    }

    public abstract boolean shouldCreateStream(String tableName);

    public abstract void cleanup();
}
