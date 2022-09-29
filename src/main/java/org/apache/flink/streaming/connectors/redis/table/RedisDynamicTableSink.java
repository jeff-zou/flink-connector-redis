package org.apache.flink.streaming.connectors.redis.table;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisConfigBase;
import org.apache.flink.streaming.connectors.redis.common.config.RedisOptions;
import org.apache.flink.streaming.connectors.redis.common.config.RedisSinkOptions;
import org.apache.flink.streaming.connectors.redis.common.hanlder.FlinkJedisConfigHandler;
import org.apache.flink.streaming.connectors.redis.common.hanlder.RedisHandlerServices;
import org.apache.flink.streaming.connectors.redis.common.hanlder.RedisMapperHandler;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisSinkMapper;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;

import java.util.Map;

/** Created by jeff.zou on 2020/9/10. */
public class RedisDynamicTableSink implements DynamicTableSink {

    private FlinkJedisConfigBase flinkJedisConfigBase;
    private RedisSinkMapper redisMapper;
    private Map<String, String> properties;
    private ReadableConfig config;
    private RedisSinkOptions redisSinkOptions;
    private Integer sinkParallelism;
    private TableSchema tableSchema;

    public RedisDynamicTableSink(
            Map<String, String> properties, TableSchema tableSchema, ReadableConfig config) {
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
        redisSinkOptions =
                new RedisSinkOptions.Builder()
                        .setMaxRetryTimes(config.get(RedisOptions.SINK_MAX_RETRIES))
                        .setRedisValueDataStructure(config.get(RedisOptions.VALUE_DATA_STRUCTURE))
                        .build();
        this.tableSchema = tableSchema;
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
                                flinkJedisConfigBase,
                                redisMapper,
                                redisSinkOptions,
                        tableSchema,
                                config)
                        : new RedisSinkFunction(
                                flinkJedisConfigBase,
                                redisMapper,
                                redisSinkOptions,
                        tableSchema);

        return SinkFunctionProvider.of(redisSinkFunction, sinkParallelism);
    }

    @Override
    public DynamicTableSink copy() {
        return new RedisDynamicTableSink(properties, tableSchema, config);
    }

    @Override
    public String asSummaryString() {
        return "REDIS";
    }
}
