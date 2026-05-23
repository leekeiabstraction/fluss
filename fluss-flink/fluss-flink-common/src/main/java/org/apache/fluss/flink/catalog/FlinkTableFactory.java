/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.flink.catalog;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.TableConfig;
import org.apache.fluss.flink.FlinkConnectorOptions;
import org.apache.fluss.flink.lake.LakeFlinkCatalog;
import org.apache.fluss.flink.lake.LakeTableFactory;
import org.apache.fluss.flink.sink.EnrichmentTableSink;
import org.apache.fluss.flink.sink.FlinkTableSink;
import org.apache.fluss.flink.sink.shuffle.DistributionMode;
import org.apache.fluss.flink.source.BinlogFlinkTableSource;
import org.apache.fluss.flink.source.ChangelogFlinkTableSource;
import org.apache.fluss.flink.source.FlinkTableSource;
import org.apache.fluss.flink.source.reader.LeaseContext;
import org.apache.fluss.flink.utils.FlinkConnectorOptionsUtils;
import org.apache.fluss.metadata.MergeEngineType;
import org.apache.fluss.metadata.TablePath;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.lookup.LookupOptions;
import org.apache.flink.table.connector.source.lookup.cache.DefaultLookupCache;
import org.apache.flink.table.connector.source.lookup.cache.LookupCache;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.logical.RowType;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.fluss.config.ConfigOptions.TABLE_DATALAKE_FORMAT;
import static org.apache.fluss.config.ConfigOptions.TABLE_DELETE_BEHAVIOR;
import static org.apache.fluss.config.FlussConfigUtils.CLIENT_PREFIX;
import static org.apache.fluss.config.FlussConfigUtils.TABLE_PREFIX;
import static org.apache.fluss.flink.catalog.FlinkCatalog.LAKE_TABLE_SPLITTER;
import static org.apache.fluss.flink.utils.FlinkConnectorOptionsUtils.getBucketKeyIndexes;
import static org.apache.fluss.flink.utils.FlinkConnectorOptionsUtils.getBucketKeys;
import static org.apache.fluss.flink.utils.FlinkConnectorOptionsUtils.validateDistributionModeForMergeEngine;
import static org.apache.fluss.flink.utils.FlinkConversions.toFlinkOption;

/** Factory to create table source and table sink for Fluss. */
public class FlinkTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    protected final LakeFlinkCatalog lakeFlinkCatalog;
    private volatile LakeTableFactory lakeTableFactory;

    public FlinkTableFactory(LakeFlinkCatalog lakeFlinkCatalog) {
        this.lakeFlinkCatalog = lakeFlinkCatalog;
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        // check whether should read from datalake
        ObjectIdentifier tableIdentifier = context.getObjectIdentifier();
        String tableName = tableIdentifier.getObjectName();
        if (tableName.contains(LAKE_TABLE_SPLITTER)) {
            // Extract the lake table name: for "table$lake" -> "table"
            // for "table$lake$snapshots" -> "table$snapshots"
            String lakeTableName = tableName.replaceFirst("\\$lake", "");

            lakeTableFactory = mayInitLakeTableFactory();
            return lakeTableFactory.createDynamicTableSource(context, lakeTableName);
        }

        // Check if this is a $changelog suffix in table name
        if (tableName.endsWith(FlinkCatalog.CHANGELOG_TABLE_SUFFIX)) {
            return createChangelogTableSource(context, tableIdentifier, tableName);
        }

        // Check if this is a $binlog suffix in table name
        if (tableName.endsWith(FlinkCatalog.BINLOG_TABLE_SUFFIX)) {
            return createBinlogTableSource(context, tableIdentifier, tableName);
        }

        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        final ReadableConfig tableOptions = helper.getOptions();
        // Phase L.3: enrichment-target tables are write-only — reject SELECT at plan time.
        rejectIfEnrichmentTarget(tableOptions, context.getObjectIdentifier());
        validateSourceOptions(tableOptions);

        boolean isStreamingMode =
                context.getConfiguration().get(ExecutionOptions.RUNTIME_MODE)
                        == RuntimeExecutionMode.STREAMING;

        RowType tableOutputType = (RowType) context.getPhysicalRowDataType().getLogicalType();

        ZoneId timeZone =
                FlinkConnectorOptionsUtils.getLocalTimeZone(
                        context.getConfiguration().get(TableConfigOptions.LOCAL_TIME_ZONE));
        final FlinkConnectorOptionsUtils.StartupOptions startupOptions =
                FlinkConnectorOptionsUtils.getStartupOptions(tableOptions, timeZone);

        ResolvedSchema resolvedSchema = context.getCatalogTable().getResolvedSchema();
        ResolvedCatalogTable resolvedCatalogTable = context.getCatalogTable();
        int[] primaryKeyIndexes = resolvedSchema.getPrimaryKeyIndexes();
        int[] partitionKeyIndexes =
                resolvedCatalogTable.getPartitionKeys().stream()
                        .mapToInt(tableOutputType::getFieldIndex)
                        .toArray();
        int[] bucketKeyIndexes = getBucketKeyIndexes(tableOptions, tableOutputType);

        // options for lookup
        LookupCache cache = null;
        LookupOptions.LookupCacheType lookupCacheType = tableOptions.get(LookupOptions.CACHE_TYPE);
        if (lookupCacheType.equals(LookupOptions.LookupCacheType.PARTIAL)) {
            cache = DefaultLookupCache.fromConfig(tableOptions);
        } else if (lookupCacheType.equals(LookupOptions.LookupCacheType.FULL)) {
            // currently, flink framework only support InputFormatProvider
            // as ScanRuntimeProviders for Full caching lookup join, so in here, we just throw
            // unsupported exception
            throw new UnsupportedOperationException("Full lookup caching is not supported yet.");
        }

        // other option values
        long partitionDiscoveryIntervalMs =
                tableOptions
                        .get(FlinkConnectorOptions.SCAN_PARTITION_DISCOVERY_INTERVAL)
                        .toMillis();

        LeaseContext leaseContext = LeaseContext.fromConf(tableOptions);
        return new FlinkTableSource(
                toFlussTablePath(context.getObjectIdentifier()),
                toFlussClientConfig(
                        context.getCatalogTable().getOptions(), context.getConfiguration()),
                toFlussTableConfig(tableOptions),
                tableOutputType,
                primaryKeyIndexes,
                bucketKeyIndexes,
                partitionKeyIndexes,
                isStreamingMode,
                startupOptions,
                tableOptions.get(FlinkConnectorOptions.LOOKUP_ASYNC),
                tableOptions.get(FlinkConnectorOptions.LOOKUP_INSERT_IF_NOT_EXISTS),
                cache,
                partitionDiscoveryIntervalMs,
                tableOptions.get(toFlinkOption(ConfigOptions.TABLE_DATALAKE_ENABLED)),
                tableOptions.get(toFlinkOption(ConfigOptions.TABLE_MERGE_ENGINE)),
                context.getCatalogTable().getOptions(),
                leaseContext);
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        final ReadableConfig tableOptions = helper.getOptions();

        // Phase L.3: write-only enrichment sink.
        if (tableOptions.getOptional(FlinkConnectorOptions.ENRICHMENT_TARGET).isPresent()
                || tableOptions.getOptional(FlinkConnectorOptions.ENRICHMENT_GROUP).isPresent()) {
            return buildEnrichmentSink(context, tableOptions);
        }

        boolean isStreamingMode =
                context.getConfiguration().get(ExecutionOptions.RUNTIME_MODE)
                        == RuntimeExecutionMode.STREAMING;

        ResolvedCatalogTable resolvedCatalogTable = context.getCatalogTable();
        List<String> partitionKeys = resolvedCatalogTable.getPartitionKeys();

        RowType rowType = (RowType) context.getPhysicalRowDataType().getLogicalType();

        MergeEngineType mergeEngineType =
                tableOptions.get(toFlinkOption(ConfigOptions.TABLE_MERGE_ENGINE));

        // For primary key tables with any merge engine, keyed shuffle must be enabled
        // to ensure correct data routing.
        int[] primaryKeyIndexes = context.getPrimaryKeyIndexes();
        DistributionMode distributionMode =
                tableOptions.get(FlinkConnectorOptions.SINK_DISTRIBUTION_MODE);
        if (primaryKeyIndexes.length > 0) {
            validateDistributionModeForMergeEngine(mergeEngineType, distributionMode);
        }

        return new FlinkTableSink(
                toFlussTablePath(context.getObjectIdentifier()),
                toFlussClientConfig(
                        context.getCatalogTable().getOptions(), context.getConfiguration()),
                rowType,
                primaryKeyIndexes,
                partitionKeys,
                isStreamingMode,
                tableOptions.get(toFlinkOption(ConfigOptions.TABLE_MERGE_ENGINE)),
                tableOptions.get(toFlinkOption(TABLE_DATALAKE_FORMAT)),
                tableOptions.get(FlinkConnectorOptions.SINK_IGNORE_DELETE),
                tableOptions.get(toFlinkOption(TABLE_DELETE_BEHAVIOR)),
                tableOptions.get(FlinkConnectorOptions.BUCKET_NUMBER),
                getBucketKeys(tableOptions),
                distributionMode,
                tableOptions.getOptional(FlinkConnectorOptions.SINK_PRODUCER_ID).orElse(null));
    }

    /**
     * Phase L.3: build the write-only enrichment sink. Validates pairing of {@code
     * enrichment.target}/{@code enrichment.group}; defers schema validation to {@link
     * EnrichmentTableSink}'s plan-time resolution.
     */
    private DynamicTableSink buildEnrichmentSink(Context context, ReadableConfig tableOptions) {
        String target =
                tableOptions
                        .getOptional(FlinkConnectorOptions.ENRICHMENT_TARGET)
                        .orElseThrow(
                                () ->
                                        new ValidationException(
                                                "'enrichment.group' requires 'enrichment.target' "
                                                        + "to also be set."));
        String group =
                tableOptions
                        .getOptional(FlinkConnectorOptions.ENRICHMENT_GROUP)
                        .orElseThrow(
                                () ->
                                        new ValidationException(
                                                "'enrichment.target' requires 'enrichment.group' "
                                                        + "to also be set."));
        if (group.isEmpty()) {
            throw new ValidationException("'enrichment.group' must not be empty.");
        }
        if (context.getPrimaryKeyIndexes().length > 0) {
            throw new ValidationException(
                    "Enrichment-target tables must be log-only; remove the PRIMARY KEY "
                            + "declaration on "
                            + context.getObjectIdentifier()
                            + ".");
        }
        TablePath sinkPath = toFlussTablePath(context.getObjectIdentifier());
        TablePath targetPath =
                parseEnrichmentTarget(
                        target, context.getObjectIdentifier().getDatabaseName(), sinkPath);
        RowType sinkRowType = (RowType) context.getPhysicalRowDataType().getLogicalType();
        return new EnrichmentTableSink(
                sinkPath,
                targetPath,
                toFlussClientConfig(
                        context.getCatalogTable().getOptions(), context.getConfiguration()),
                group,
                sinkRowType);
    }

    private static TablePath parseEnrichmentTarget(
            String target, String defaultDatabase, TablePath sinkPath) {
        if (target.isEmpty()) {
            throw new ValidationException("'enrichment.target' must not be empty.");
        }
        String[] parts = target.split("\\.", -1);
        TablePath parsed;
        if (parts.length == 1) {
            parsed = TablePath.of(defaultDatabase, parts[0]);
        } else if (parts.length == 2) {
            parsed = TablePath.of(parts[0], parts[1]);
        } else {
            throw new ValidationException(
                    "'enrichment.target' must be of the form `<db>.<table>` or `<table>`, "
                            + "but got: "
                            + target);
        }
        if (parsed.equals(sinkPath)) {
            throw new ValidationException(
                    "'enrichment.target' cannot point at the enrichment-target table itself ("
                            + sinkPath
                            + ").");
        }
        return parsed;
    }

    private static void rejectIfEnrichmentTarget(
            ReadableConfig tableOptions, ObjectIdentifier identifier) {
        if (tableOptions.getOptional(FlinkConnectorOptions.ENRICHMENT_TARGET).isPresent()) {
            String target = tableOptions.get(FlinkConnectorOptions.ENRICHMENT_TARGET);
            throw new ValidationException(
                    "Table "
                            + identifier
                            + " is a write-only enrichment target for column group '"
                            + tableOptions
                                    .getOptional(FlinkConnectorOptions.ENRICHMENT_GROUP)
                                    .orElse("?")
                            + "' on "
                            + target
                            + ". To read enriched data, query "
                            + target
                            + " directly.");
        }
    }

    @Override
    public String factoryIdentifier() {
        return FlinkCatalogFactory.IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return new HashSet<>(Collections.singletonList(FlinkConnectorOptions.BOOTSTRAP_SERVERS));
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        HashSet<ConfigOption<?>> options =
                new HashSet<>(
                        Arrays.asList(
                                FlinkConnectorOptions.AUTO_INCREMENT_FIELDS,
                                FlinkConnectorOptions.BUCKET_KEY,
                                FlinkConnectorOptions.BUCKET_NUMBER,
                                FlinkConnectorOptions.SCAN_STARTUP_MODE,
                                FlinkConnectorOptions.SCAN_STARTUP_TIMESTAMP,
                                FlinkConnectorOptions.SCAN_PARTITION_DISCOVERY_INTERVAL,
                                FlinkConnectorOptions.SCAN_KV_SNAPSHOT_LEASE_ID,
                                FlinkConnectorOptions.SCAN_KV_SNAPSHOT_LEASE_DURATION,
                                FlinkConnectorOptions.LOOKUP_ASYNC,
                                FlinkConnectorOptions.LOOKUP_INSERT_IF_NOT_EXISTS,
                                FlinkConnectorOptions.SINK_IGNORE_DELETE,
                                FlinkConnectorOptions.SINK_BUCKET_SHUFFLE,
                                FlinkConnectorOptions.SINK_DISTRIBUTION_MODE,
                                FlinkConnectorOptions.SINK_PRODUCER_ID,
                                FlinkConnectorOptions.ENRICHMENT_TARGET,
                                FlinkConnectorOptions.ENRICHMENT_GROUP,
                                LookupOptions.MAX_RETRIES,
                                LookupOptions.CACHE_TYPE,
                                LookupOptions.PARTIAL_CACHE_EXPIRE_AFTER_ACCESS,
                                LookupOptions.PARTIAL_CACHE_EXPIRE_AFTER_WRITE,
                                LookupOptions.PARTIAL_CACHE_CACHE_MISSING_KEY,
                                LookupOptions.PARTIAL_CACHE_MAX_ROWS));
        // forward all fluss table and client options
        options.addAll(FlinkConnectorOptions.TABLE_OPTIONS);
        options.addAll(FlinkConnectorOptions.CLIENT_OPTIONS);
        return options;
    }

    private static Configuration toFlussClientConfig(
            Map<String, String> tableOptions, ReadableConfig flinkConfig) {
        Configuration flussConfig = new Configuration();
        flussConfig.setString(
                ConfigOptions.BOOTSTRAP_SERVERS.key(),
                tableOptions.get(FlinkConnectorOptions.BOOTSTRAP_SERVERS.key()));

        // forward all client configs
        tableOptions.forEach(
                (key, value) -> {
                    if (key.startsWith(CLIENT_PREFIX)) {
                        flussConfig.setString(key, value);
                    }
                });

        // Todo support LookupOptions.MAX_RETRIES. Currently, Fluss doesn't support connector level
        // retry. The option 'client.lookup.max-retries' is only for dealing with the
        // RetriableException return by server not all exceptions. Trace by:
        // https://github.com/apache/fluss/issues/2099
        return flussConfig;
    }

    private static TableConfig toFlussTableConfig(ReadableConfig tableOptions) {
        Configuration tableConfig = new Configuration();

        // forward all table-level configs by iterating through known table options
        // this approach is safer than using toMap() which may not exist in all Flink versions
        for (ConfigOption<?> option : FlinkConnectorOptions.TABLE_OPTIONS) {
            if (option.key().startsWith(TABLE_PREFIX)) {
                Object value = tableOptions.getOptional(option).orElse(null);
                if (value != null) {
                    // convert value to string for configuration storage
                    tableConfig.setString(option.key(), value.toString());
                }
            }
        }

        return new TableConfig(tableConfig);
    }

    private static TablePath toFlussTablePath(ObjectIdentifier tablePath) {
        return TablePath.of(tablePath.getDatabaseName(), tablePath.getObjectName());
    }

    private LakeTableFactory mayInitLakeTableFactory() {
        if (lakeTableFactory == null) {
            synchronized (this) {
                if (lakeTableFactory == null) {
                    lakeTableFactory = new LakeTableFactory(lakeFlinkCatalog);
                }
            }
        }
        return lakeTableFactory;
    }

    /**
     * Validates table source options explicitly recognized by Flink.
     *
     * @param tableOptions the table options to validate
     */
    private static void validateSourceOptions(ReadableConfig tableOptions) {
        FlinkConnectorOptionsUtils.validateTableSourceOptions(tableOptions);
    }

    /** Creates a ChangelogFlinkTableSource for $changelog virtual tables. */
    private DynamicTableSource createChangelogTableSource(
            Context context, ObjectIdentifier tableIdentifier, String tableName) {
        // Extract the base table name by removing the $changelog suffix
        String baseTableName =
                tableName.substring(
                        0, tableName.length() - FlinkCatalog.CHANGELOG_TABLE_SUFFIX.length());

        boolean isStreamingMode =
                context.getConfiguration().get(ExecutionOptions.RUNTIME_MODE)
                        == RuntimeExecutionMode.STREAMING;

        // tableOutputType includes metadata columns: [_change_type, _log_offset, _commit_timestamp,
        // data_cols...]
        RowType tableOutputType = (RowType) context.getPhysicalRowDataType().getLogicalType();

        // Extract data columns type (skip the 3 metadata columns) for index calculations
        int numMetadataColumns = 3;
        List<RowType.RowField> dataFields =
                tableOutputType
                        .getFields()
                        .subList(numMetadataColumns, tableOutputType.getFieldCount());
        RowType dataColumnsType = new RowType(new ArrayList<>(dataFields));

        Map<String, String> catalogTableOptions = context.getCatalogTable().getOptions();
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        final ReadableConfig tableOptions = helper.getOptions();
        validateSourceOptions(tableOptions);

        ZoneId timeZone =
                FlinkConnectorOptionsUtils.getLocalTimeZone(
                        context.getConfiguration().get(TableConfigOptions.LOCAL_TIME_ZONE));
        final FlinkConnectorOptionsUtils.StartupOptions startupOptions =
                FlinkConnectorOptionsUtils.getStartupOptions(tableOptions, timeZone);

        ResolvedCatalogTable resolvedCatalogTable = context.getCatalogTable();

        // Partition key indexes based on data columns
        int[] partitionKeyIndexes =
                resolvedCatalogTable.getPartitionKeys().stream()
                        .mapToInt(dataColumnsType::getFieldIndex)
                        .toArray();

        long partitionDiscoveryIntervalMs =
                tableOptions
                        .get(FlinkConnectorOptions.SCAN_PARTITION_DISCOVERY_INTERVAL)
                        .toMillis();

        return new ChangelogFlinkTableSource(
                TablePath.of(tableIdentifier.getDatabaseName(), baseTableName),
                toFlussClientConfig(catalogTableOptions, context.getConfiguration()),
                tableOutputType,
                partitionKeyIndexes,
                isStreamingMode,
                startupOptions,
                partitionDiscoveryIntervalMs,
                catalogTableOptions);
    }

    /** Creates a BinlogFlinkTableSource for $binlog virtual tables. */
    private DynamicTableSource createBinlogTableSource(
            Context context, ObjectIdentifier tableIdentifier, String tableName) {
        // Extract the base table name by removing the $binlog suffix
        String baseTableName =
                tableName.substring(
                        0, tableName.length() - FlinkCatalog.BINLOG_TABLE_SUFFIX.length());

        boolean isStreamingMode =
                context.getConfiguration().get(ExecutionOptions.RUNTIME_MODE)
                        == RuntimeExecutionMode.STREAMING;

        // tableOutputType: [_change_type, _log_offset, _commit_timestamp, before ROW<...>, after
        // ROW<...>]
        RowType tableOutputType = (RowType) context.getPhysicalRowDataType().getLogicalType();

        Map<String, String> catalogTableOptions = context.getCatalogTable().getOptions();
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        final ReadableConfig tableOptions = helper.getOptions();
        validateSourceOptions(tableOptions);

        ZoneId timeZone =
                FlinkConnectorOptionsUtils.getLocalTimeZone(
                        context.getConfiguration().get(TableConfigOptions.LOCAL_TIME_ZONE));
        final FlinkConnectorOptionsUtils.StartupOptions startupOptions =
                FlinkConnectorOptionsUtils.getStartupOptions(tableOptions, timeZone);

        // Check if the table is partitioned from the internal option
        boolean isPartitioned =
                tableOptions.get(FlinkConnectorOptions.INTERNAL_BINLOG_IS_PARTITIONED);

        long partitionDiscoveryIntervalMs =
                tableOptions
                        .get(FlinkConnectorOptions.SCAN_PARTITION_DISCOVERY_INTERVAL)
                        .toMillis();

        return new BinlogFlinkTableSource(
                TablePath.of(tableIdentifier.getDatabaseName(), baseTableName),
                toFlussClientConfig(catalogTableOptions, context.getConfiguration()),
                tableOutputType,
                isPartitioned,
                isStreamingMode,
                startupOptions,
                partitionDiscoveryIntervalMs,
                catalogTableOptions);
    }
}
