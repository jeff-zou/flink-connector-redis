package org.apache.flink.streaming.connectors.redis.table;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisConfigBase;
import org.apache.flink.streaming.connectors.redis.common.config.RedisLookupOptions;
import org.apache.flink.streaming.connectors.redis.common.config.RedisOptions;
import org.apache.flink.streaming.connectors.redis.common.hanlder.FlinkJedisConfigHandler;
import org.apache.flink.streaming.connectors.redis.common.hanlder.RedisHandlerServices;
import org.apache.flink.streaming.connectors.redis.common.hanlder.RedisMapperHandler;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.TableFunctionProvider;
import org.apache.flink.util.Preconditions;

import java.util.Map;

/** redis dynamic table source. @Author: jeff.zou @Date: 2022/3/7.13:41 */
public class RedisDynamicTableSource implements LookupTableSource {

    private FlinkJedisConfigBase flinkJedisConfigBase;
    private Map<String, String> properties;
    private TableSchema tableSchema;
    private ReadableConfig config;
    private RedisMapper redisMapper;
    private RedisLookupOptions redisCacheOptions;

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
        return TableFunctionProvider.of(
                new RedisLookupFunction(
                        flinkJedisConfigBase, redisMapper, redisCacheOptions, tableSchema));
    }

    public RedisDynamicTableSource(
            Map<String, String> properties, TableSchema tableSchema, ReadableConfig config) {
        this.properties = properties;
        Preconditions.checkNotNull(properties, "properties should not be null");
        this.tableSchema = tableSchema;
        Preconditions.checkNotNull(tableSchema, "resolvedSchema should not be null");
        this.config = config;

        redisMapper =
                RedisHandlerServices.findRedisHandler(RedisMapperHandler.class, properties)
                        .createRedisMapper(config);
        this.properties = properties;
        this.tableSchema = tableSchema;
        this.config = config;
        flinkJedisConfigBase =
                RedisHandlerServices.findRedisHandler(FlinkJedisConfigHandler.class, properties)
                        .createFlinkJedisConfig(config);
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
        return new RedisDynamicTableSource(properties, tableSchema, config);
    }

    @Override
    public String asSummaryString() {
        return "REDIS";
    }
}
