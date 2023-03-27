package com.wepay.kafka.connect.bigquery.constants;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.common.collect.ImmutableMap;
import org.apache.kafka.connect.data.Schema;

public class SchemaConstants {

    /**
     * Map for Record Schema to Standard SQL type conversion
     */
    public static final ImmutableMap<Schema.Type, LegacySQLTypeName> PRIMITIVE_TYPE_MAP =
    ImmutableMap.of(
        Schema.Type.BOOLEAN,
            LegacySQLTypeName.BOOLEAN,
        Schema.Type.FLOAT32,
            LegacySQLTypeName.FLOAT,
        Schema.Type.FLOAT64,
            LegacySQLTypeName.FLOAT,
        Schema.Type.INT8,
            LegacySQLTypeName.INTEGER,
        Schema.Type.INT16,
            LegacySQLTypeName.INTEGER,
        Schema.Type.INT32,
            LegacySQLTypeName.INTEGER,
        Schema.Type.INT64,
            LegacySQLTypeName.INTEGER,
        Schema.Type.STRING,
            LegacySQLTypeName.STRING,
        Schema.Type.BYTES,
            LegacySQLTypeName.BYTES
    );

    public static final ImmutableMap<Field.Mode, TableFieldSchema.Mode> BQTableSchemaModeMap =
            ImmutableMap.of(
                    Field.Mode.NULLABLE, TableFieldSchema.Mode.NULLABLE,
                    Field.Mode.REPEATED, TableFieldSchema.Mode.REPEATED,
                    Field.Mode.REQUIRED, TableFieldSchema.Mode.REQUIRED);

    public static final ImmutableMap<StandardSQLTypeName, TableFieldSchema.Type> BQTableSchemaTypeMap =
            new ImmutableMap.Builder<StandardSQLTypeName, TableFieldSchema.Type>()
                    .put(StandardSQLTypeName.BOOL, TableFieldSchema.Type.BOOL)
                    .put(StandardSQLTypeName.BYTES, TableFieldSchema.Type.BYTES)
                    .put(StandardSQLTypeName.DATE, TableFieldSchema.Type.DATE)
                    .put(StandardSQLTypeName.DATETIME, TableFieldSchema.Type.DATETIME)
                    .put(StandardSQLTypeName.FLOAT64, TableFieldSchema.Type.DOUBLE)
                    .put(StandardSQLTypeName.GEOGRAPHY, TableFieldSchema.Type.GEOGRAPHY)
                    .put(StandardSQLTypeName.INT64, TableFieldSchema.Type.INT64)
                    .put(StandardSQLTypeName.NUMERIC, TableFieldSchema.Type.NUMERIC)
                    .put(StandardSQLTypeName.STRING, TableFieldSchema.Type.STRING)
                    .put(StandardSQLTypeName.STRUCT, TableFieldSchema.Type.STRUCT)
                    .put(StandardSQLTypeName.TIME, TableFieldSchema.Type.TIME)
                    .put(StandardSQLTypeName.TIMESTAMP, TableFieldSchema.Type.TIMESTAMP)
                    .build();
}
