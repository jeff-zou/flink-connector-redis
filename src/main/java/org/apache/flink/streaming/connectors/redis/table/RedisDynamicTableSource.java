package org.apache.flink.streaming.connectors.redis.table;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkConfigBase;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkConfigHandler;
import org.apache.flink.streaming.connectors.redis.common.config.RedisOptions;
import org.apache.flink.streaming.connectors.redis.common.config.RedisQueryOptions;
import org.apache.flink.streaming.connectors.redis.common.hanlder.RedisHandlerServices;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.streaming.connectors.redis.common.mapper.RowRedisQueryMapper;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.AsyncTableFunctionProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;

import java.util.Map;

/** redis dynamic table source. @Author: jeff.zou @Date: 2022/3/7.13:41 */
public class RedisDynamicTableSource implements ScanTableSource, LookupTableSource {

    private FlinkConfigBase flinkConfigBase;
    private Map<String, String> properties;
    private ResolvedSchema resolvedSchema;
    private ReadableConfig config;
    private RedisMapper redisMapper;
    private RedisQueryOptions redisQueryOptions;

    protected DataType producedDataType;

    private RedisCommand redisCommand;

    public RedisDynamicTableSource(
            RedisCommand redisCommand,
            DataType physicalDataType,
            Map<String, String> properties,
            ResolvedSchema resolvedSchema,
            ReadableConfig config) {
        this.redisCommand = redisCommand;
        this.properties = properties;
        Preconditions.checkNotNull(properties, "properties should not be null");
        this.resolvedSchema = resolvedSchema;
        Preconditions.checkNotNull(resolvedSchema, "resolvedSchema should not be null");
        this.config = config;
        this.producedDataType = physicalDataType;
        redisMapper = new RowRedisQueryMapper(redisCommand);
        this.properties = properties;
        this.resolvedSchema = resolvedSchema;
        this.config = config;
        flinkConfigBase =
                RedisHandlerServices.findRedisHandler(FlinkConfigHandler.class, properties)
                        .createFlinkConfig(config);
        redisQueryOptions =
                new RedisQueryOptions.Builder()
                        .setCacheTTL(config.get(RedisOptions.LOOKUP_CHCHE_TTL))
                        .setCacheMaxSize(config.get(RedisOptions.LOOKUP_CACHE_MAX_ROWS))
                        .setMaxRetryTimes(config.get(RedisOptions.LOOKUP_MAX_RETRIES))
                        .setLoadAll(config.get(RedisOptions.LOOKUP_CACHE_LOAD_ALL))
                        .setRedisValueDataStructure(config.get(RedisOptions.VALUE_DATA_STRUCTURE))
                        .build();
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.all();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        RedisSourceFunction redisSourceFunction =
                new RedisSourceFunction<>(
                        redisMapper, config, flinkConfigBase, redisQueryOptions, resolvedSchema);
        return SourceFunctionProvider.of(redisSourceFunction, true);
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
        return AsyncTableFunctionProvider.of(
                new RedisLookupFunction(
                        flinkConfigBase, redisMapper, redisQueryOptions, resolvedSchema));
    }

    @Override
    public DynamicTableSource copy() {
        return new RedisDynamicTableSource(
                redisCommand, producedDataType, properties, resolvedSchema, config);
    }

    @Override
    public String asSummaryString() {
        return "REDIS";
    }
}
