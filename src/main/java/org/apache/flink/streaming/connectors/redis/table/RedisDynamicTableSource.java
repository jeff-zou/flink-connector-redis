package org.apache.flink.streaming.connectors.redis.table;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.connectors.redis.command.RedisCommand;
import org.apache.flink.streaming.connectors.redis.config.FlinkConfigBase;
import org.apache.flink.streaming.connectors.redis.config.FlinkConfigHandler;
import org.apache.flink.streaming.connectors.redis.config.RedisJoinConfig;
import org.apache.flink.streaming.connectors.redis.config.RedisOptions;
import org.apache.flink.streaming.connectors.redis.hanlder.RedisHandlerServices;
import org.apache.flink.streaming.connectors.redis.mapper.RedisMapper;
import org.apache.flink.streaming.connectors.redis.mapper.RowRedisQueryMapper;
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
    private RedisJoinConfig redisJoinConfig;

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
        redisJoinConfig =
                new RedisJoinConfig.Builder()
                        .setCacheTTL(config.get(RedisOptions.LOOKUP_CHCHE_TTL))
                        .setCacheMaxSize(config.get(RedisOptions.LOOKUP_CACHE_MAX_ROWS))
                        .setLoadAll(config.get(RedisOptions.LOOKUP_CACHE_LOAD_ALL))
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
                        redisMapper, config, flinkConfigBase, redisJoinConfig, resolvedSchema);
        return SourceFunctionProvider.of(
                redisSourceFunction, this.redisCommand.isCommandBoundedness());
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
        return AsyncTableFunctionProvider.of(
                new RedisLookupFunction(
                        flinkConfigBase, redisMapper, redisJoinConfig, resolvedSchema, config));
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
