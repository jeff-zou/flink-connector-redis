package org.apache.flink.streaming.connectors.redis.table;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkConfigBase;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkConfigHandler;
import org.apache.flink.streaming.connectors.redis.common.config.RedisOptions;
import org.apache.flink.streaming.connectors.redis.common.config.RedisSinkOptions;
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

/** Created by jeff.zou on 2020/9/10. */
public class RedisDynamicTableSink implements DynamicTableSink {

    private FlinkConfigBase flinkConfigBase;
    private RedisSinkMapper redisMapper;
    private Map<String, String> properties;
    private ReadableConfig config;
    private RedisSinkOptions redisSinkOptions;
    private Integer sinkParallelism;
    private ResolvedSchema resolvedSchema;

    public RedisDynamicTableSink(
            Map<String, String> properties, ResolvedSchema resolvedSchema, ReadableConfig config) {
        this.properties = properties;
        Preconditions.checkNotNull(properties, "properties should not be null");
        this.config = config;
        this.sinkParallelism = config.get(RedisOptions.SINK_PARALLELISM);
        redisMapper =
                (RedisSinkMapper)
                        RedisHandlerServices.findRedisHandler(RedisMapperHandler.class, properties)
                                .createRedisMapper(config);
        flinkConfigBase =
                RedisHandlerServices.findRedisHandler(FlinkConfigHandler.class, properties)
                        .createFlinkConfig(config);
        redisSinkOptions =
                new RedisSinkOptions.Builder()
                        .setMaxRetryTimes(config.get(RedisOptions.SINK_MAX_RETRIES))
                        .setRedisValueDataStructure(config.get(RedisOptions.VALUE_DATA_STRUCTURE))
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
        RedisSinkFunction redisSinkFunction =
                config.get(RedisOptions.SINK_LIMIT)
                        ? new RedisLimitedSinkFunction(
                                flinkConfigBase,
                                redisMapper,
                                redisSinkOptions,
                                resolvedSchema,
                                config)
                        : new RedisSinkFunction(
                                flinkConfigBase, redisMapper, redisSinkOptions, resolvedSchema);

        return SinkFunctionProvider.of(redisSinkFunction, sinkParallelism);
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
