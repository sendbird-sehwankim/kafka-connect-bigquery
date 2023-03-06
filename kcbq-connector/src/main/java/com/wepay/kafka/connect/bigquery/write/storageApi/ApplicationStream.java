package com.wepay.kafka.connect.bigquery.write.storageApi;

import com.google.cloud.bigquery.storage.v1.*;
import com.google.protobuf.Descriptors;
import com.wepay.kafka.connect.bigquery.exception.BigQueryConnectException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class ApplicationStream {
    StreamState currentState = null;
    Logger logger = LoggerFactory.getLogger(ApplicationStream.class);
    private final String tableName;
    private WriteStream stream = null;
    private JsonStreamWriter jsonWriter = null;
    private Map<TopicPartition, OffsetAndMetadata> offsetInformation;

    public Map<TopicPartition, OffsetAndMetadata> getOffsetInformation() {
        return offsetInformation;
    }

    public void setOffsetInformation(Map<TopicPartition, OffsetAndMetadata> offsetInformation) {
        this.offsetInformation = offsetInformation;
    }

    private final BigQueryWriteClient client;
    /**
     * Number of times append is called
     */
    private AtomicInteger appendCallsCount;

    /**
     * Number of append requests completed successfully. This can never be greater than appendCallsCount
     */
    private AtomicInteger completedCallsCount;

    /**
     * This is called by builder to guarantee sequence.
     */
    private AtomicInteger maxCallsCount;
    public ApplicationStream(String tableName, BigQueryWriteClient client) {
        this.client = client;
        this.tableName = tableName;
        try {
            generateStream();
        } catch (Exception e) {
            throw new BigQueryConnectException("Stream creation failed : " + e.getMessage());
        }
        this.offsetInformation = new HashMap<>();
        this.appendCallsCount = new AtomicInteger();
        this.maxCallsCount = new AtomicInteger();
        this.completedCallsCount = new AtomicInteger();
    }

    private void generateStream() throws Descriptors.DescriptorValidationException, IOException, InterruptedException {
        this.stream = client.createWriteStream(tableName, WriteStream.newBuilder().setType(WriteStream.Type.PENDING).build());
        this.jsonWriter = JsonStreamWriter.newBuilder(stream.getName(), client).build();
        currentState = StreamState.CREATED;
    }

    public void closeStream() {
        this.jsonWriter.close();
        logger.info("JSON Writer closed for stream : "+getStreamName());
    }
    public String getStreamName() {
        return this.stream.getName();
    }

    public void increaseAppendCallCount() {
        this.appendCallsCount.incrementAndGet();
    }

    public int increaseMaxCallsCount() {
        int count = this.maxCallsCount.incrementAndGet();
        if(currentState == StreamState.CREATED) {
            currentState = StreamState.APPEND;
        }
        return count;
    }

    public void increaseCompletedCallsCount() {
        this.completedCallsCount.incrementAndGet();
    }

    public boolean canBeMovedToInactive() {
        return currentState != StreamState.CREATED;
    }

    public void updateOffsetInformation(Map<TopicPartition, OffsetAndMetadata> newOffsets) {
        offsetInformation.putAll(newOffsets);
        increaseMaxCallsCount();
    }

    public JsonStreamWriter writer() {
        return this.jsonWriter;
    }

    public boolean areAllExpectedCallsCompleted(){
        return (this.maxCallsCount.intValue() == this.appendCallsCount.intValue()) && (this.appendCallsCount.intValue() == this.completedCallsCount.intValue()) ;
    }

    public void finalise() {
        if(currentState == StreamState.APPEND) {
            FinalizeWriteStreamResponse finalizeResponse =
                    client.finalizeWriteStream(this.getStreamName());
            logger.info("Rows written: " + finalizeResponse.getRowCount());
            currentState = StreamState.FINALISED;
        } else {
            logger.warn("Stream could not be finalised as current state "+ currentState+ " is not expected state.");
        }
    }

    public void commit() {
        if(currentState == StreamState.FINALISED) {
            BatchCommitWriteStreamsRequest commitRequest =
                    BatchCommitWriteStreamsRequest.newBuilder()
                            .setParent(tableName)
                            .addWriteStreams(getStreamName())
                            .build();
            BatchCommitWriteStreamsResponse commitResponse = client.batchCommitWriteStreams(commitRequest);
            // If the response does not have a commit time, it means the commit operation failed.
            if (!commitResponse.hasCommitTime()) {
                for (StorageError err : commitResponse.getStreamErrorsList()) {
                    logger.error(err.getErrorMessage());
                }
                throw new RuntimeException("Error committing the streams");
            }
            logger.info("Appended and committed records successfully.");
            currentState = StreamState.COMMITTED;
        }
    }

    public boolean isReadyForCommit() {
        return currentState == StreamState.COMMITTED;
    }

    public void markInactive() {
        currentState = StreamState.INACTIVE;
    }

    @Override
    public String toString() {
        return "ApplicationStream{" +
                "currentState=" + currentState +
                ", tableName='" + tableName + '\'' +
                ", offsetInformation=" + offsetInformation +
                ", appendCallsCount=" + appendCallsCount +
                ", completedCallsCount=" + completedCallsCount +
                ", maxCallsCount=" + maxCallsCount +
                '}';
    }

    public boolean isInactive() {
        return currentState == StreamState.INACTIVE;
    }
}
