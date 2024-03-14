package org.apache.flink.streaming.connectors.redis.table;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkConfigBase;
import org.apache.flink.streaming.connectors.redis.common.config.RedisQueryOptions;
import org.apache.flink.streaming.connectors.redis.common.config.RedisValueDataStructure;
import org.apache.flink.streaming.connectors.redis.common.container.RedisCommandsContainer;
import org.apache.flink.streaming.connectors.redis.common.container.RedisCommandsContainerBuilder;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandBaseDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class RedisSourceFunction<T> extends RichSourceFunction<T> {

    private static final Logger LOG = LoggerFactory.getLogger(RedisSourceFunction.class);

    ReadableConfig config;

    private FlinkConfigBase flinkConfigBase;
    private RedisCommandsContainer redisCommandsContainer;

    private final int maxRetryTimes;

    private RedisCommand redisCommand;

    private final RedisValueDataStructure redisValueDataStructure;

    private final List<DataType> dataTypes;

    public RedisSourceFunction(
            RedisMapper redisMapper,
            ReadableConfig config,
            FlinkConfigBase flinkConfigBase,
            RedisQueryOptions redisQueryOptions,
            ResolvedSchema resolvedSchema) {
        this.config = config;
        this.flinkConfigBase = flinkConfigBase;
        this.maxRetryTimes = redisQueryOptions.getMaxRetryTimes();
        this.redisValueDataStructure = redisQueryOptions.getRedisValueDataStructure();
        RedisCommandBaseDescription redisCommandDescription = redisMapper.getCommandDescription();
        Preconditions.checkNotNull(
                redisCommandDescription, "Redis Mapper data type description can not be null");
        this.redisCommand = redisCommandDescription.getRedisCommand();
        this.dataTypes = resolvedSchema.getColumnDataTypes();
    }

    @Override
    public void open(Configuration parameters) throws Exception {

        try {
            this.redisCommandsContainer = RedisCommandsContainerBuilder.build(this.flinkConfigBase);
            this.redisCommandsContainer.open();
            LOG.info("{} success to create redis container:{}", Thread.currentThread().getId());
        } catch (Exception e) {
            LOG.error("Redis has not been properly initialized: ", e);
            throw e;
        }

        super.open(parameters);
    }

    @Override
    public void run(SourceContext ctx) throws Exception {
        String[] keys = new String[3];
        keys[0] = "test";
        // It will try many times which less than {@code maxRetryTimes} until execute success.
        for (int i = 0; i <= maxRetryTimes; i++) {
            try {
                query(ctx, keys);
                break;
            } catch (Exception e) {
                LOG.error("query redis error, retry times:{}", i, e);
                if (i >= maxRetryTimes) {
                    throw new RuntimeException("query redis error ", e);
                }
                Thread.sleep(500 * i);
            }
        }
    }

    private void query(SourceContext ctx, String[] keys) throws Exception {
        switch (redisCommand.getQueryCommand()) {
            case GET:
                {
                    String result = this.redisCommandsContainer.get(String.valueOf(keys[0])).get();
                    GenericRowData rowData =
                            RedisResultWrapper.createRowDataForString(
                                    keys, result, redisValueDataStructure, dataTypes);
                    ctx.collect(rowData);
                    break;
                }
            case HGET:
                {
                    String result =
                            this.redisCommandsContainer
                                    .hget(String.valueOf(keys[0]), String.valueOf(keys[1]))
                                    .get();
                    GenericRowData rowData =
                            RedisResultWrapper.createRowDataForHash(
                                    keys, result, redisValueDataStructure, dataTypes);
                    ctx.collect(rowData);
                    break;
                }
            default:
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (redisCommandsContainer != null) {
            redisCommandsContainer.close();
        }
    }

    @Override
    public void cancel() {}
}
