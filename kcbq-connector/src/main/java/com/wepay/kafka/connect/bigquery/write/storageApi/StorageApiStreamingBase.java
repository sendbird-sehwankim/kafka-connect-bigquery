package com.wepay.kafka.connect.bigquery.write.storageApi;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.Credentials;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteSettings;
import com.google.cloud.bigquery.storage.v1.TableName;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.protobuf.Descriptors;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public abstract class StorageApiStreamingBase {

    private final BigQueryWriteSettings writeSettings;

    public StorageApiStreamingBase(BigQueryWriteSettings writeSettings) {
        this.writeSettings = writeSettings;
    }
    abstract public void initialize(TableName tableName, Schema schema) throws IOException, Descriptors.DescriptorValidationException, InterruptedException;

    abstract public void appendRows(TableName tableName, Schema schema, SinkRecord row) ;


//    abstract public Map<TopicPartition, OffsetAndMetadata> getOffsets();
    public BigQueryWriteSettings getWriteSettings() {
        return writeSettings;
    }

}
