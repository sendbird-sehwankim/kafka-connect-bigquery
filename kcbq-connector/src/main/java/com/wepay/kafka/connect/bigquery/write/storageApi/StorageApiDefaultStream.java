package com.wepay.kafka.connect.bigquery.write.storageApi;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.cloud.bigquery.*;
import com.google.cloud.bigquery.storage.v1.*;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.Descriptors;
import com.wepay.kafka.connect.bigquery.api.KafkaSchemaRecordType;
import com.wepay.kafka.connect.bigquery.convert.RecordConverter;
import com.wepay.kafka.connect.bigquery.convert.SchemaConverter;
import com.wepay.kafka.connect.bigquery.convert.WriteApiRecordConverter;
import com.wepay.kafka.connect.bigquery.convert.WriteApiSchemaConverter;
import com.wepay.kafka.connect.bigquery.utils.SinkRecordConverter;
import io.grpc.Status;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

public class StorageApiDefaultStream extends StorageApiStreamingBase {
    private static final Logger logger = LoggerFactory.getLogger(StorageApiDefaultStream.class);
    ConcurrentMap<String, JsonStreamWriter> map = new ConcurrentHashMap<>();

    public StorageApiDefaultStream(BigQuery bigQuery, int retry, long retryWait, BigQueryWriteSettings writeSettings) {
        super(bigQuery, retry, retryWait, writeSettings);
    }

    @Override
    public void appendRows(TableName tableName, TableSchema recordSchema, List<Object[]> rows) {
       // JsonStreamWriter writer;
        JSONArray jsonArr = new JSONArray();
        try {
            if(!map.containsKey(tableName.toString())) {
                if(recordSchema == null) {
                    JSONObject convertedRecord = (JSONObject) rows.get(0)[1];
                    recordSchema = new WriteApiSchemaConverter().convertSchemaless(convertedRecord);
                }
                map.putIfAbsent(tableName.toString(),JsonStreamWriter.newBuilder(tableName.toString(), recordSchema, getWriteClient()).build());
            }

        } catch (Descriptors.DescriptorValidationException | IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
        JsonStreamWriter writer = map.get(tableName.toString());
        try{
            rows.forEach(item -> jsonArr.put(item[1]));
            //long identifier = System.nanoTime();
            logger.debug("Sending records to write Api");
            ApiFuture<AppendRowsResponse> response = writer.append(jsonArr);
            AppendRowsResponse writeResult = response.get();
           // logger.debug("Result received "+identifier);
            if (writeResult.hasError()) {
                logger.info("Error Status : " + writeResult.getError());
                if (writeResult.getRowErrorsCount() > 0) {
                    for (RowError error : writeResult.getRowErrorsList()) {
                        logger.info("Row " + error.getIndex() + " has error : " + error.getMessage());
                    }
                }
            }
            if (writeResult.hasUpdatedSchema()) {
                logger.info("Schema updates are not yet allowed.");
            }
            /**
             * Collect the response
             * check if isDone() and remove from collection and return
             * if not , then wait for it to finish
             * listen to callback and on completion, remove from collection and notify //ApiFutures.addCallback(response, new WriterCallback(row), MoreExecutors.directExecutor());
             */
        } catch (Descriptors.DescriptorValidationException | InterruptedException | IOException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            logger.info(e.getCause().getMessage());
            if (e.getCause() instanceof Exceptions.AppendSerializtionError){
                Exceptions.AppendSerializtionError  exception = (Exceptions.AppendSerializtionError) e.getCause();
                if(exception.getStatus().getCode().equals(Status.Code.INVALID_ARGUMENT)){
                    // User actionable error
                    for(Map.Entry rowIndexToError: exception.getRowIndexToErrorMessage().entrySet()) {
                        logger.info("User actionable error on : "+ jsonArr.put(rowIndexToError.getKey()) +"  as the data hit an issue : " + rowIndexToError.getValue());
                    }

                } else {
                    logger.info("Exception is not due to invalid argument");
                    logger.error(exception.getStatus().getDescription());
                    throw new RuntimeException(e);
                }
            } else {
                logger.info("Exception is not of type Exceptions.AppendSerializtionError");
                logger.error(e.getCause().getMessage());
                throw new RuntimeException(e);
            }
        }
    }

}
