package org.apache.flink.streaming.connectors.redis.table;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkConfigBase;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkConfigHandler;
import org.apache.flink.streaming.connectors.redis.common.config.RedisLookupOptions;
import org.apache.flink.streaming.connectors.redis.common.config.RedisOptions;
import org.apache.flink.streaming.connectors.redis.common.hanlder.RedisHandlerServices;
import org.apache.flink.streaming.connectors.redis.common.hanlder.RedisMapperHandler;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.source.AsyncTableFunctionProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.util.Preconditions;

import java.util.Map;

/** redis dynamic table source. @Author: jeff.zou @Date: 2022/3/7.13:41 */
public class RedisDynamicTableSource implements LookupTableSource {

    private FlinkConfigBase flinkConfigBase;
    private Map<String, String> properties;
    private ResolvedSchema resolvedSchema;
    private ReadableConfig config;
    private RedisMapper redisMapper;
    private RedisLookupOptions redisCacheOptions;

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
        return AsyncTableFunctionProvider.of(
                new RedisLookupFunction(
                        flinkConfigBase, redisMapper, redisCacheOptions, resolvedSchema));
    }

    public RedisDynamicTableSource(
            Map<String, String> properties, ResolvedSchema resolvedSchema, ReadableConfig config) {
        this.properties = properties;
        Preconditions.checkNotNull(properties, "properties should not be null");
        this.resolvedSchema = resolvedSchema;
        Preconditions.checkNotNull(resolvedSchema, "resolvedSchema should not be null");
        this.config = config;

        redisMapper =
                RedisHandlerServices.findRedisHandler(RedisMapperHandler.class, properties)
                        .createRedisMapper(config);
        this.properties = properties;
        this.resolvedSchema = resolvedSchema;
        this.config = config;
        flinkConfigBase =
                RedisHandlerServices.findRedisHandler(FlinkConfigHandler.class, properties)
                        .createFlinkConfig(config);
        redisCacheOptions =
                new RedisLookupOptions.Builder()
                        .setCacheTTL(config.get(RedisOptions.LOOKUP_CHCHE_TTL))
                        .setCacheMaxSize(config.get(RedisOptions.LOOKUP_CACHE_MAX_ROWS))
                        .setMaxRetryTimes(config.get(RedisOptions.LOOKUP_MAX_RETRIES))
                        .setLoadAll(config.get(RedisOptions.LOOKUP_CACHE_LOAD_ALL))
                        .setRedisValueDataStructure(config.get(RedisOptions.VALUE_DATA_STRUCTURE))
                        .build();
    }

    @Override
    public DynamicTableSource copy() {
        return new RedisDynamicTableSource(properties, resolvedSchema, config);
    }

    @Override
    public String asSummaryString() {
        return "REDIS";
    }
}
