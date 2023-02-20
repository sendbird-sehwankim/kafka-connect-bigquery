package com.wepay.kafka.connect.bigquery.write.storageApi;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class StorageApiBatchModeHandler {

    Logger logger = LoggerFactory.getLogger(StorageApiBatchModeHandler.class);
    private final StorageApiApplicationStreams streamApi;
    public StorageApiBatchModeHandler(StorageApiApplicationStreams streamApi) {
        this.streamApi = streamApi;
    }

    public String getCurrentStream(String tableName) {
        return this.streamApi.getCurrentStreamForTable(tableName);
    }

    public void createNewStream() {
     logger.info(" I am called at a scheduled time");
    }

    public void updateOffsetsForStream(String streamName, Map<TopicPartition, OffsetAndMetadata> offsetInfo) {
        this.streamApi.updateOffsetsForStream(streamName, offsetInfo);
    }

    public Map<TopicPartition, OffsetAndMetadata> getCommitableOffsets() {
        return null;
    }

}
