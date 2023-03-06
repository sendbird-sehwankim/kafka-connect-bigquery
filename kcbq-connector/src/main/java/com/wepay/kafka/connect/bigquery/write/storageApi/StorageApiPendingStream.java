package com.wepay.kafka.connect.bigquery.write.storageApi;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteSettings;
import com.google.cloud.bigquery.storage.v1.TableName;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.Descriptors;
import com.wepay.kafka.connect.bigquery.exception.BigQueryConnectException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.json.JSONArray;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class StorageApiPendingStream extends StorageApiApplicationStreams {


    ConcurrentMap<String, LinkedHashMap<String, ApplicationStream>> streams;
    ConcurrentMap<String, String> currentStreams;

    public StorageApiPendingStream(BigQuery bigQuery, int retry, long retryWait, BigQueryWriteSettings writeSettings) {
        super(bigQuery, retry, retryWait, writeSettings);
        streams = new ConcurrentHashMap<>();
        currentStreams = new ConcurrentHashMap<>();
    }

    @Override
    public void appendRows(TableName tableName, TableSchema recordSchema, List<Object[]> rows, String streamName) {
        JSONArray jsonArray = new JSONArray();
        rows.forEach(item -> jsonArray.put(item[1]));
        ApplicationStream applicationStream = this.streams.get(tableName.toString()).get(streamName);
        try {
            ApiFuture<AppendRowsResponse> response = applicationStream.writer().append(jsonArray);
            applicationStream.increaseAppendCallCount();
            ApiFutures.addCallback(response, new StorageApiBatchCallbackHandler(applicationStream, tableName.toString()), MoreExecutors.directExecutor());
        } catch (IOException | Descriptors.DescriptorValidationException e) {
            throw new BigQueryConnectException(e);
        }
    }

    @Override
    public void createStream(String tableName) {
        String oldStream;
        synchronized (this) {
            // Create new Stream
            if (!streams.containsKey(tableName))
                this.streams.put(tableName, new LinkedHashMap<>());
            ApplicationStream stream = new ApplicationStream(tableName, getWriteClient());
            String streamName = stream.getStreamName();
            this.streams.get(tableName).put(streamName, stream);
            oldStream = this.currentStreams.get(tableName);
            this.currentStreams.put(tableName, streamName);
        }
        if (oldStream != null)
            commitStreamIfEligible(tableName, oldStream);
    }

    public void finaliseAndCommitStream(ApplicationStream stream) {
        stream.finalise();
        stream.commit();
    }

    @Override
    public String getCurrentStreamForTable(String tableName) {
        if (!currentStreams.containsKey(tableName)) {
            synchronized (this) {
                if (!currentStreams.containsKey(tableName)) {
                    this.createStream(tableName);
                }
            }

        }
        return Objects.requireNonNull(this.currentStreams.get(tableName));
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> getCommitableOffsets() {
        Map<TopicPartition, OffsetAndMetadata> offsetsReadyForCommits = new ConcurrentHashMap<>();
        streams.values()
                .forEach(map -> {
                            int i = 0;
                            for (ApplicationStream applicationStream : map.values()) {
                                i++;
                                logger.info("StreamName " + applicationStream.getStreamName() + ", all expected calls done ? " + applicationStream.areAllExpectedCallsCompleted());
                                if(applicationStream.isInactive()) {
                                    continue;
                                }
                                if (applicationStream.isReadyForCommit()) {
                                    offsetsReadyForCommits.putAll(applicationStream.getOffsetInformation());
                                    applicationStream.markInactive();
                                    //map.remove(applicationStream.getStreamName());
                                    logger.info("################### committed (item " + i + " in list out of " + map.size() + " items), Stream info " + applicationStream);
                                } else {
                                    logger.info("################### Not ready for commit (item " + i + " in list out of " + map.size() + " items), Stream info " + applicationStream);
                                    // We move sequentially for offset commit, until current offsets are ready, we cannot commit next.
                                    break;
                                }

                            }
                        }
                );
        logger.info("Pending stream was called to return commitableOffsets : " + offsetsReadyForCommits);
        return offsetsReadyForCommits;
    }


    @Override
    public boolean shouldCreateStream(String tableName) {
        boolean result;
        synchronized (this) {
            String streamName = this.currentStreams.get(tableName);
            result = (streamName == null) || this.streams.get(tableName).get(streamName).canBeMovedToInactive();
            if (result) {
                this.createStream(tableName);
            }
            return result;
        }
    }

    @Override
    public void updateOffsetsForStream(String tableName, String streamName, Map<TopicPartition, OffsetAndMetadata> offsetInfo) {
        this.streams.get(tableName).get(streamName).updateOffsetInformation(offsetInfo);
    }

    private void commitStreamIfEligible(String tableName, String streamName) {
        if (!currentStreams.getOrDefault(tableName, "").equals(streamName)) {
            // We are done with all expected calls for non-active streams, lets finalise and commit the stream.
            ApplicationStream stream = this.streams.get(tableName).get(streamName);
            if (stream.areAllExpectedCallsCompleted())
                finaliseAndCommitStream(stream);
        }
    }

    public void cleanup() {
        this.streams.values()
                .stream().flatMap(item -> item.values().stream())
                .collect(Collectors.toList())
                .forEach(ApplicationStream::closeStream);
    }

    public class StorageApiBatchCallbackHandler implements ApiFutureCallback<AppendRowsResponse> {

        private final ApplicationStream stream;
        private final String tableName;

        StorageApiBatchCallbackHandler(ApplicationStream stream, String tableName) {
            this.stream = stream;
            this.tableName = tableName;
        }

        @Override
        public void onFailure(Throwable t) {
            logger.info(t.getMessage());
            throw new BigQueryConnectException("Call failed");
        }

        @Override
        public void onSuccess(AppendRowsResponse result) {
            logger.info("Stream call passed");
            stream.increaseCompletedCallsCount();
            String streamName = stream.getStreamName();
            commitStreamIfEligible(tableName, streamName);
        }
    }
}
