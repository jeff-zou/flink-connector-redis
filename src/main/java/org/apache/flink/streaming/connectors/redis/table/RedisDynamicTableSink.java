package org.apache.flink.streaming.connectors.redis.table;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisConfigBase;
import org.apache.flink.streaming.connectors.redis.common.config.RedisCacheOptions;
import org.apache.flink.streaming.connectors.redis.common.config.RedisOptions;
import org.apache.flink.streaming.connectors.redis.common.hanlder.FlinkJedisConfigHandler;
import org.apache.flink.streaming.connectors.redis.common.hanlder.RedisHandlerServices;
import org.apache.flink.streaming.connectors.redis.common.hanlder.RedisMapperHandler;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisSinkMapper;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;

import java.util.Map;

import static org.apache.flink.util.Preconditions.checkState;

/** Created by jeff.zou on 2020/9/10. */
public class RedisDynamicTableSink implements DynamicTableSink {

    private FlinkJedisConfigBase flinkJedisConfigBase;
    private RedisSinkMapper redisMapper;
    private Map<String, String> properties = null;
    private ReadableConfig config;
    private RedisCacheOptions redisCacheOptions;
    private Integer sinkParallelism;
    private ResolvedSchema resolvedSchema;

    public RedisDynamicTableSink(Map<String, String> properties, ResolvedSchema resolvedSchema, ReadableConfig config) {
        this.properties = properties;
        Preconditions.checkNotNull(properties, "properties should not be null");
        this.config = config;
        this.sinkParallelism = config.get(RedisOptions.SINK_PARALLELISM);
        redisMapper =
                (RedisSinkMapper)
                        RedisHandlerServices.findRedisHandler(RedisMapperHandler.class, properties)
                                .createRedisMapper(config);
        flinkJedisConfigBase =
                RedisHandlerServices.findRedisHandler(FlinkJedisConfigHandler.class, properties)
                        .createFlinkJedisConfig(config);
        redisCacheOptions =
                new RedisCacheOptions.Builder()
                        .setCacheTTL(config.get(RedisOptions.SINK_CHCHE_TTL))
                        .setCacheMaxSize(config.get(RedisOptions.SINK_CACHE_MAX_ROWS))
                        .setMaxRetryTimes(config.get(RedisOptions.SINK_MAX_RETRIES))
                        .build();
        this.resolvedSchema = resolvedSchema;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.DELETE)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .build();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        return SinkFunctionProvider.of(
                new RedisSinkFunction(flinkJedisConfigBase, redisMapper, redisCacheOptions, resolvedSchema),
                sinkParallelism);
    }

    @Override
    public DynamicTableSink copy() {
        return new RedisDynamicTableSink(properties, resolvedSchema, config);
    }

    @Override
    public String asSummaryString() {
        return "REDIS";
    }
}
