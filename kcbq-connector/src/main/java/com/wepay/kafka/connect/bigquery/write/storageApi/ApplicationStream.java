package com.wepay.kafka.connect.bigquery.write.storageApi;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;
import java.util.Map;

public class ApplicationStream {
    private String streamName;
    private Map<TopicPartition, OffsetAndMetadata> offsetInformation;
    private int appendCallCount;

    public ApplicationStream(String streamName) {
        this.streamName = streamName;
        this.offsetInformation = new HashMap<>();
        this.appendCallCount = 0;
    }

    public String getStreamName() {
        return streamName;
    }

    public void setStreamName(String streamName) {
        this.streamName = streamName;
    }

    public Map<TopicPartition, OffsetAndMetadata> getOffsetInformation() {
        return offsetInformation;
    }

    public void setOffsetInformation(Map<TopicPartition, OffsetAndMetadata> offsetInformation) {
        this.offsetInformation = offsetInformation;
    }

    public int getAppendCallCount() {
        return appendCallCount;
    }

    public void setAppendCallCount(int appendCallCount) {
        this.appendCallCount = appendCallCount;
    }
}
