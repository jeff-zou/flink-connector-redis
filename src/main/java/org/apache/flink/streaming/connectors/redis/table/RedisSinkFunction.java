package org.apache.flink.streaming.connectors.redis.table;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisConfigBase;
import org.apache.flink.streaming.connectors.redis.common.config.RedisCacheOptions;
import org.apache.flink.streaming.connectors.redis.common.container.RedisCommandsContainer;
import org.apache.flink.streaming.connectors.redis.common.container.RedisCommandsContainerBuilder;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisDataType;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisOperationType;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisSinkMapper;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/** @param <IN> */
public class RedisSinkFunction<IN> extends RichSinkFunction<IN> {

    private static final Logger LOG = LoggerFactory.getLogger(RedisSinkFunction.class);

    private Integer ttl;

    private RedisSinkMapper<IN> redisSinkMapper;
    private RedisCommand redisCommand;

    private FlinkJedisConfigBase flinkJedisConfigBase;
    private RedisCommandsContainer redisCommandsContainer;

    private final int maxRetryTimes;
    private List<DataType> columnDataTypes;

    /**
     * Creates a new {@link RedisSinkFunction} that connects to the Redis server.
     *
     * @param flinkJedisConfigBase The configuration of {@link FlinkJedisConfigBase}
     * @param redisSinkMapper This is used to generate Redis command and key value from incoming
     *     elements.
     */
    public RedisSinkFunction(
            FlinkJedisConfigBase flinkJedisConfigBase,
            RedisSinkMapper<IN> redisSinkMapper,
            RedisCacheOptions redisCacheOptions,
            ResolvedSchema resolvedSchema) {
        Objects.requireNonNull(
                flinkJedisConfigBase, "Redis connection pool config should not be null");
        Objects.requireNonNull(redisSinkMapper, "Redis Mapper can not be null");
        Objects.requireNonNull(
                redisSinkMapper.getCommandDescription(),
                "Redis Mapper data type description can not be null");

        this.flinkJedisConfigBase = flinkJedisConfigBase;
        this.maxRetryTimes = redisCacheOptions.getMaxRetryTimes();
        this.redisSinkMapper = redisSinkMapper;
        RedisCommandDescription redisCommandDescription =
                (RedisCommandDescription) redisSinkMapper.getCommandDescription();

        this.redisCommand = redisCommandDescription.getRedisCommand();
        this.ttl = redisCommandDescription.getTTL();
        this.columnDataTypes = resolvedSchema.getColumnDataTypes();
    }

    /**
     * Called when new data arrives to the sink, and forwards it to Redis channel. Depending on the
     * specified Redis data type (see {@link RedisDataType}), a different Redis command will be
     * applied. Available commands are RPUSH, LPUSH, SADD, PUBLISH, SET, SETEX, PFADD, HSET, ZADD.
     *
     * @param input The incoming data
     */
    @Override
    public void invoke(IN input, Context context) throws Exception {
        RowData rowData = (RowData) input;
        RowKind kind = rowData.getRowKind();
        if (kind != RowKind.INSERT && kind != RowKind.UPDATE_AFTER) {
            return;
        }

        String key =
                redisSinkMapper.getKeyFromData(rowData, columnDataTypes.get(0).getLogicalType(), 0);
        if (redisCommand.getRedisOperationType() == RedisOperationType.DEL) {
            deleteInvoke(key, rowData);
            return;
        }

        String value =
                redisSinkMapper.getValueFromData(
                        rowData, columnDataTypes.get(1).getLogicalType(), 1);
        String field = null;
        if (redisCommand.getRedisDataType() == RedisDataType.HASH
                || redisCommand.getRedisDataType() == RedisDataType.SORTED_SET) {
            field =
                    redisSinkMapper.getFieldFromData(
                            rowData, columnDataTypes.get(1).getLogicalType(), 1);
            value =
                    redisSinkMapper.getValueFromData(
                            rowData, columnDataTypes.get(2).getLogicalType(), 2);
        }

        for (int i = 0; i <= this.maxRetryTimes; i++) {
            try {
                sink(key, field, value);
                if (ttl != null) {
                    this.redisCommandsContainer.expire(key, ttl);
                }
                break;
            } catch (UnsupportedOperationException e) {
                throw e;
            } catch (Exception e1) {
                LOG.error("sink redis error, retry times:{}", i, e1);
                if (i >= this.maxRetryTimes) {
                    throw new RuntimeException("sink redis error ", e1);
                }
                Thread.sleep(500 * i);
            }
        }
    }

    private void deleteInvoke(String key, RowData rowData) throws Exception {
        String field = null;
        if (redisCommand != RedisCommand.DEL) {
            field =
                    redisSinkMapper.getFieldFromData(
                            rowData, columnDataTypes.get(1).getLogicalType(), 1);
        }

        for (int i = 0; i <= maxRetryTimes; i++) {
            try {
                sink(key, field, null);
                break;
            } catch (UnsupportedOperationException e) {
                throw e;
            } catch (Exception e1) {
                LOG.error("sink redis error, retry times:{}", i, e1);
                if (i >= this.maxRetryTimes) {
                    throw new RuntimeException("sink redis error ", e1);
                }
                Thread.sleep(500 * i);
            }
        }
    }

    private void sink(String key, String field, String value) throws Exception {
        switch (redisCommand) {
            case RPUSH:
                this.redisCommandsContainer.rpush(key, value);
                break;
            case LPUSH:
                this.redisCommandsContainer.lpush(key, value);
                break;
            case SADD:
                this.redisCommandsContainer.sadd(key, value);
                break;
            case SET:
                this.redisCommandsContainer.set(key, value);
                break;
            case PFADD:
                this.redisCommandsContainer.pfadd(key, value);
                break;
            case PUBLISH:
                this.redisCommandsContainer.publish(key, value);
                break;
            case ZADD:
                this.redisCommandsContainer.zadd(key, field, value);
                break;
            case ZINCRBY:
                this.redisCommandsContainer.zincrBy(key, field, value);
                break;
            case ZREM:
                this.redisCommandsContainer.zrem(key, field);
                break;
            case SREM:
                this.redisCommandsContainer.srem(key, field);
                break;
            case HSET:
                this.redisCommandsContainer.hset(key, field, value);
                break;
            case HINCRBY:
                this.redisCommandsContainer.hincrBy(key, field, Long.valueOf(value));
                break;
            case INCRBY:
                this.redisCommandsContainer.incrBy(key, Long.valueOf(value));
                break;
            case DECRBY:
                this.redisCommandsContainer.decrBy(key, Long.valueOf(value));
                break;
            case DEL:
                this.redisCommandsContainer.del(key);
                break;
            case HDEL:
                this.redisCommandsContainer.hdel(key, field);
                break;
            default:
                throw new UnsupportedOperationException(
                        "Cannot process such data type: " + redisCommand);
        }
    }

    /**
     * Initializes the connection to Redis by either cluster or sentinels or single server.
     *
     * @throws IllegalArgumentException if jedisPoolConfig, jedisClusterConfig and
     *     jedisSentinelConfig are all null
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        try {
            this.redisCommandsContainer =
                    RedisCommandsContainerBuilder.build(this.flinkJedisConfigBase);
            this.redisCommandsContainer.open();
            LOG.info("success to create redis container:{}", this.flinkJedisConfigBase.toString());
        } catch (Exception e) {
            LOG.error("Redis has not been properly initialized: ", e);
            throw e;
        }
    }

    /**
     * Closes commands container.
     *
     * @throws IOException if command container is unable to close.
     */
    @Override
    public void close() throws IOException {
        if (redisCommandsContainer != null) {
            redisCommandsContainer.close();
        }
    }
}
