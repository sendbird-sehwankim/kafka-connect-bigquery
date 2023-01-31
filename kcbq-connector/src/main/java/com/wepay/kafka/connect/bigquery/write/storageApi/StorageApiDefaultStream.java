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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class StorageApiDefaultStream extends StorageApiStreamingBase {
    private static final Logger logger = LoggerFactory.getLogger(StorageApiDefaultStream.class);
    private JsonStreamWriter writer;

    Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
    Set<Object> responses = new HashSet<>();
    private boolean initialised = false;
    private final SchemaConverter<TableSchema, Schema> writeApiSchemaConverter;

    private final RecordConverter<JSONObject> recordConverter = new WriteApiRecordConverter();
    public StorageApiDefaultStream(BigQueryWriteSettings writeSettings, SchemaConverter<TableSchema, Schema> schemaConverter) {
        super(writeSettings);
        this.writeApiSchemaConverter = schemaConverter;
        //this.recordConverter =
    }


    @Override
    public void initialize(TableName tableName, Schema tableSchema) throws IOException, Descriptors.DescriptorValidationException, InterruptedException {
        BigQueryWriteClient writeClient = BigQueryWriteClient.create(getWriteSettings());
        mayBeCreateTable(tableName, tableSchema);

        BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
        Table table = bigquery.getTable(tableName.getDataset(), tableName.getTable());
        Schema schema = table.getDefinition().getSchema();
        TableSchema recordSchema = writeApiSchemaConverter.convertSchema(schema);

        writer = JsonStreamWriter.newBuilder(tableName.toString(), recordSchema, writeClient).build();
        initialised = true;
    }

    public void mayBeCreateTable(TableName tableName, Schema tableSchema) {
        BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

        // Create a table that uses this schema.
        TableId tableId = TableId.of(tableName.getProject(), tableName.getDataset(), tableName.getTable());
        Table table = bigquery.getTable(tableId);
        if (table == null) {
            TableInfo tableInfo =
                    TableInfo.newBuilder(tableId, StandardTableDefinition.of(tableSchema)).build();
            bigquery.create(tableInfo);
        }
    }

    @Override
    public void appendRows(TableName tableName, Schema tableSchema, SinkRecord row) {
        try {
            if (!initialised)
                initialize(tableName, tableSchema);
        } catch (IOException | Descriptors.DescriptorValidationException | InterruptedException e) {
            throw new RuntimeException(e);
        }
        JSONArray jsonArr = new JSONArray();
        jsonArr.put(recordConverter.convertRecord(row, KafkaSchemaRecordType.VALUE));
        try {
            ApiFuture<AppendRowsResponse> response = writer.append(jsonArr);
            AppendRowsResponse writeResult = response.get();
            logger.info("Result : " + writeResult);
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
//    @Override
//    public Map<TopicPartition, OffsetAndMetadata> getOffsets() {
//        System.out.println("Offsets currently saved "+ offsets.values());
//       return offsets;
//    }

//    public class WriterCallback implements ApiFutureCallback<AppendRowsResponse> {
//
//        SinkRecord row;
//        WriterCallback(SinkRecord row) {
//         this.row = row;
//        }
//        @Override
//        public void onFailure(Throwable t) {
//            System.out.println(t.getMessage());
//        }
//
//
//        @Override
//        public void onSuccess(AppendRowsResponse result) {
//            System.out.println("Offset : " + row.kafkaOffset());
//            offsets.put(
//                    new TopicPartition(row.topic(), row.kafkaPartition()),
//                    new OffsetAndMetadata(row.kafkaOffset()));
//            responses.remove(this.row);
//        }
//    }
}
