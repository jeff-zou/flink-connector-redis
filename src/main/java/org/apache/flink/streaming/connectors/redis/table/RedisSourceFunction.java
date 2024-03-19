package org.apache.flink.streaming.connectors.redis.table;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.connectors.redis.command.RedisCommand;
import org.apache.flink.streaming.connectors.redis.command.RedisCommandBaseDescription;
import org.apache.flink.streaming.connectors.redis.command.RedisSelectCommand;
import org.apache.flink.streaming.connectors.redis.config.FlinkConfigBase;
import org.apache.flink.streaming.connectors.redis.config.RedisOptions;
import org.apache.flink.streaming.connectors.redis.config.RedisQueryOptions;
import org.apache.flink.streaming.connectors.redis.config.RedisValueDataStructure;
import org.apache.flink.streaming.connectors.redis.container.RedisCommandsContainer;
import org.apache.flink.streaming.connectors.redis.container.RedisCommandsContainerBuilder;
import org.apache.flink.streaming.connectors.redis.mapper.RedisMapper;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class RedisSourceFunction<T> extends RichSourceFunction<T> {

    private static final Logger LOG = LoggerFactory.getLogger(RedisSourceFunction.class);

    ReadableConfig readableConfig;

    private FlinkConfigBase flinkConfigBase;
    private RedisCommandsContainer redisCommandsContainer;

    private final int maxRetryTimes;

    private RedisCommand redisCommand;

    private final RedisValueDataStructure redisValueDataStructure;

    private final List<DataType> dataTypes;

    private final String[] queryParameter;

    public RedisSourceFunction(
            RedisMapper redisMapper,
            ReadableConfig readableConfig,
            FlinkConfigBase flinkConfigBase,
            RedisQueryOptions redisQueryOptions,
            ResolvedSchema resolvedSchema) {
        this.readableConfig = readableConfig;
        this.flinkConfigBase = flinkConfigBase;
        this.maxRetryTimes = redisQueryOptions.getMaxRetryTimes();
        this.redisValueDataStructure = redisQueryOptions.getRedisValueDataStructure();
        this.queryParameter = new String[2];
        RedisCommandBaseDescription redisCommandDescription = redisMapper.getCommandDescription();
        Preconditions.checkNotNull(
                redisCommandDescription, "Redis Mapper data type description can not be null");
        this.redisCommand = redisCommandDescription.getRedisCommand();
        this.dataTypes = resolvedSchema.getColumnDataTypes();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        Preconditions.checkNotNull(
                this.readableConfig.get(RedisOptions.SCAN_KEY),
                "the scan.key for source can not be null");
        this.queryParameter[0] = this.readableConfig.get(RedisOptions.SCAN_KEY);

        Preconditions.checkArgument(
                redisCommand.getSelectCommand() != RedisSelectCommand.NONE,
                String.format("the command %s do not support query.", redisCommand.name()));

        if (redisCommand.getSelectCommand() == RedisSelectCommand.HGET) {
            Preconditions.checkNotNull(
                    this.readableConfig.get(RedisOptions.SCAN_ADDITION_KEY),
                    "must set field value of Map to scan.addition.key");
            this.queryParameter[1] = this.readableConfig.get(RedisOptions.SCAN_ADDITION_KEY);
        } else if (redisCommand.getSelectCommand() == RedisSelectCommand.ZSCORE) {
            Preconditions.checkNotNull(
                    this.readableConfig.get(RedisOptions.SCAN_ADDITION_KEY),
                    "must set member value of SortedSet to scan.addition.key");
            Preconditions.checkArgument(
                    dataTypes.get(1).getLogicalType() instanceof DoubleType,
                    "the second column's type of source table must be double. the type of score is double when the data structure in redis is SortedSet.");
            this.queryParameter[1] = this.readableConfig.get(RedisOptions.SCAN_ADDITION_KEY);
        }

        try {
            this.redisCommandsContainer = RedisCommandsContainerBuilder.build(this.flinkConfigBase);
            this.redisCommandsContainer.open();
            LOG.info("success to create redis container.");
        } catch (Exception e) {
            LOG.error("Redis has not been properly initialized: ", e);
            throw e;
        }
        super.open(parameters);
    }

    @Override
    public void run(SourceContext ctx) throws Exception {
        // It will try many times which less than {@code maxRetryTimes} until execute success.
        for (int i = 0; i <= maxRetryTimes; i++) {
            try {
                query(ctx);
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

    private void query(SourceContext ctx) throws Exception {
        switch (redisCommand.getSelectCommand()) {
            case GET:
                {
                    String result = this.redisCommandsContainer.get(queryParameter[0]).get();
                    GenericRowData rowData =
                            RedisResultWrapper.createRowDataForString(
                                    queryParameter, result, redisValueDataStructure, dataTypes);
                    ctx.collect(rowData);
                    break;
                }
            case HGET:
                {
                    String result =
                            this.redisCommandsContainer
                                    .hget(queryParameter[0], queryParameter[1])
                                    .get();
                    GenericRowData rowData =
                            RedisResultWrapper.createRowDataForHash(
                                    queryParameter, result, redisValueDataStructure, dataTypes);
                    ctx.collect(rowData);
                    break;
                }
            case ZSCORE:
                {
                    Double result =
                            this.redisCommandsContainer
                                    .zscore(queryParameter[0], queryParameter[1])
                                    .get();
                    GenericRowData rowData =
                            RedisResultWrapper.createRowDataForSortedSet(
                                    queryParameter, result, dataTypes);
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
