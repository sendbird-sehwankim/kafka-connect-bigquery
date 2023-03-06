package com.wepay.kafka.connect.bigquery.write.storageApi;

import com.google.cloud.bigquery.storage.v1.TableName;
import com.wepay.kafka.connect.bigquery.utils.TableNameUtils;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Array;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class StorageApiBatchModeHandler {

    Logger logger = LoggerFactory.getLogger(StorageApiBatchModeHandler.class);
    private final StorageApiApplicationStreams streamApi;

    private final List<String> tableNames;
    public StorageApiBatchModeHandler(StorageApiApplicationStreams streamApi, List<String> topics, String dataset, String projectId) {
        this.streamApi = streamApi;
        tableNames = topics.stream().map(topic -> TableName.of(projectId, dataset, topic).toString()).collect(Collectors.toList());
    }

    public String getCurrentStream(String tableName) {
        String streamName = this.streamApi.getCurrentStreamForTable(tableName);
        logger.debug("Current stream for table "+ tableName+" is "+ streamName);
        return streamName;
    }

    public void createNewStream() {
        logger.info("Schduled call");
        tableNames.forEach(this::createNewStreamForTable);
    }

    public void createNewStreamForTable(String tableName) {
        if(streamApi.shouldCreateStream(tableName)) {
            logger.info("Created new stream for table "+ tableName);
        } else {
            logger.info("Not creating stream for table "+ tableName);
        }
    }
    public void updateOffsetsForStream(String tableName, String streamName, Map<TopicPartition, OffsetAndMetadata> offsetInfo) {
        this.streamApi.updateOffsetsForStream(tableName, streamName, offsetInfo);
    }

    public Map<TopicPartition, OffsetAndMetadata> getCommitableOffsets() {
        return this.streamApi.getCommitableOffsets();
    }

    public void shutdown() {
        this.streamApi.cleanup();
    }
}
