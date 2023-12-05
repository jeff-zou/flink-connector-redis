package org.apache.flink.streaming.connectors.redis.table;

import io.lettuce.core.RedisFuture;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkConfigBase;
import org.apache.flink.streaming.connectors.redis.common.config.RedisSinkOptions;
import org.apache.flink.streaming.connectors.redis.common.config.RedisValueDataStructure;
import org.apache.flink.streaming.connectors.redis.common.container.RedisCommandsContainer;
import org.apache.flink.streaming.connectors.redis.common.container.RedisCommandsContainerBuilder;
import org.apache.flink.streaming.connectors.redis.common.converter.RedisRowConverter;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisDataType;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisOperationType;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisSinkMapper;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalTime;
import java.util.List;
import java.util.Objects;

/**
 * @param <IN>
 */
public class RedisSinkFunction<IN> extends RichSinkFunction<IN> {

    private static final Logger LOG = LoggerFactory.getLogger(RedisSinkFunction.class);

    protected Integer ttl;

    private boolean setIfAbsent;

    private boolean ttlKeyNotAbsent;

    protected int expireTimeSeconds = -1;

    private RedisSinkMapper<IN> redisSinkMapper;
    private RedisCommand redisCommand;

    private FlinkConfigBase flinkConfigBase;
    private RedisCommandsContainer redisCommandsContainer;

    private final int maxRetryTimes;
    private List<DataType> columnDataTypes;

    private RedisValueDataStructure redisValueDataStructure;

    /**
     * Creates a new {@link RedisSinkFunction} that connects to the Redis server.
     *
     * @param flinkConfigBase The configuration of {@link FlinkConfigBase}
     * @param redisSinkMapper This is used to generate Redis command and key value from incoming
     *     elements.
     */
    public RedisSinkFunction(
            FlinkConfigBase flinkConfigBase,
            RedisSinkMapper<IN> redisSinkMapper,
            RedisSinkOptions redisSinkOptions,
            ResolvedSchema resolvedSchema) {
        Objects.requireNonNull(flinkConfigBase, "Redis connection pool config should not be null");
        Objects.requireNonNull(redisSinkMapper, "Redis Mapper can not be null");

        this.flinkConfigBase = flinkConfigBase;
        this.maxRetryTimes = redisSinkOptions.getMaxRetryTimes();
        this.redisSinkMapper = redisSinkMapper;
        RedisCommandDescription redisCommandDescription =
                (RedisCommandDescription) redisSinkMapper.getCommandDescription();
        Preconditions.checkNotNull(
                redisCommandDescription, "Redis Mapper data type description can not be null");

        this.redisCommand = redisCommandDescription.getRedisCommand();
        this.ttl = redisCommandDescription.getTTL();
        this.ttlKeyNotAbsent = redisCommandDescription.getTtlKeyNotAbsent();
        this.setIfAbsent = redisCommandDescription.getSetIfAbsent();
        if (redisCommandDescription.getExpireTime() != null) {
            this.expireTimeSeconds = redisCommandDescription.getExpireTime().toSecondOfDay();
        }

        this.columnDataTypes = resolvedSchema.getColumnDataTypes();
        this.redisValueDataStructure = redisSinkOptions.getRedisValueDataStructure();
        if (redisValueDataStructure == RedisValueDataStructure.row) {
            Preconditions.checkArgument(
                    this.redisCommand.getRedisOperationType() == RedisOperationType.INSERT,
                    "the value data structure cant be row when command is %s",
                    this.redisCommand.name());
        }
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
        if (kind == RowKind.UPDATE_BEFORE) {
            return;
        }

        String[] params = new String[calcParamNumByCommand()];
        for (int i = 0; i < params.length; i++) {
            params[i] =
                    redisSinkMapper.getKeyFromData(
                            rowData, columnDataTypes.get(i).getLogicalType(), i);
        }

        // the value is taken from the entire row when redisValueFromType is row, and columns
        // separated by '\01'
        if (redisValueDataStructure == RedisValueDataStructure.row) {
            params[params.length - 1] = serializeWholeRow(rowData);
        }

        startSink(params, kind);
    }

    /**
     * It will try many times which less than {@code maxRetryTimes} until execute success.
     *
     * @param params
     * @throws Exception
     */
    private void startSink(String[] params, RowKind kind) throws Exception {
        for (int i = 0; i <= maxRetryTimes; i++) {
            try {
                RedisFuture redisFuture = null;
                if (kind == RowKind.DELETE) {
                    redisFuture = rowKindDelete(params);
                } else {
                    redisFuture = sink(params);
                }

                if (redisFuture != null) {
                    redisFuture.whenComplete((r, t) -> setTtl(params[0]));
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

    /**
     * process redis command.
     *
     * @param params
     */
    private RedisFuture sink(String[] params) {
        RedisFuture redisFuture = null;
        switch (redisCommand) {
            case RPUSH:
                redisFuture = this.redisCommandsContainer.rpush(params[0], params[1]);
                break;
            case LPUSH:
                redisFuture = this.redisCommandsContainer.lpush(params[0], params[1]);
                break;
            case SADD:
                redisFuture = this.redisCommandsContainer.sadd(params[0], params[1]);
                break;
            case SET:
                {
                    if (!this.setIfAbsent) {
                        redisFuture = this.redisCommandsContainer.set(params[0], params[1]);
                    } else {
                        redisFuture = this.redisCommandsContainer.exists(params[0]);
                        redisFuture.whenComplete(
                                (existsVal, throwable) -> {
                                    if ((int) existsVal == 0) {
                                        this.redisCommandsContainer.set(params[0], params[1]);
                                    }
                                });
                    }
                }
                break;
            case PFADD:
                redisFuture = this.redisCommandsContainer.pfadd(params[0], params[1]);
                break;
            case PUBLISH:
                redisFuture = this.redisCommandsContainer.publish(params[0], params[1]);
                break;
            case ZADD:
                redisFuture = this.redisCommandsContainer.zadd(params[0], params[1], params[2]);
                break;
            case ZINCRBY:
                redisFuture = this.redisCommandsContainer.zincrBy(params[0], params[1], params[2]);
                break;
            case ZREM:
                redisFuture = this.redisCommandsContainer.zrem(params[0], params[1]);
                break;
            case SREM:
                redisFuture = this.redisCommandsContainer.srem(params[0], params[1]);
                break;
            case HSET:
                {
                    if (!this.setIfAbsent) {
                        redisFuture =
                                this.redisCommandsContainer.hset(params[0], params[1], params[2]);
                    } else {
                        redisFuture = this.redisCommandsContainer.hexists(params[0], params[1]);
                        redisFuture.whenComplete(
                                (exist, throwable) -> {
                                    if (!(Boolean) exist) {
                                        this.redisCommandsContainer.hset(
                                                params[0], params[1], params[2]);
                                    }
                                });
                    }
                }
                break;
            case HINCRBY:
                redisFuture =
                        this.redisCommandsContainer.hincrBy(
                                params[0], params[1], Long.valueOf(params[2]));
                break;
            case HINCRBYFLOAT:
                redisFuture =
                        this.redisCommandsContainer.hincrByFloat(
                                params[0], params[1], Double.valueOf(params[2]));
                break;
            case INCRBY:
                redisFuture =
                        this.redisCommandsContainer.incrBy(params[0], Long.valueOf(params[1]));
                break;
            case INCRBYFLOAT:
                redisFuture =
                        this.redisCommandsContainer.incrByFloat(
                                params[0], Double.valueOf(params[1]));
                break;
            case DECRBY:
                redisFuture =
                        this.redisCommandsContainer.decrBy(params[0], Long.valueOf(params[1]));
                break;
            case DEL:
                redisFuture = this.redisCommandsContainer.del(params[0]);
                break;
            case HDEL:
                redisFuture = this.redisCommandsContainer.hdel(params[0], params[1]);
                break;
            default:
                throw new UnsupportedOperationException(
                        "Cannot process such data type: " + redisCommand);
        }
        return redisFuture;
    }

    /**
     * process redis command when RowKind == DELETE.
     *
     * @param params
     */
    private RedisFuture rowKindDelete(String[] params) {
        RedisFuture redisFuture = null;
        switch (redisCommand) {
            case SADD:
                redisFuture = this.redisCommandsContainer.srem(params[0], params[1]);
                break;
            case SET:
                redisFuture = this.redisCommandsContainer.del(params[0]);
                break;
            case ZADD:
                redisFuture = this.redisCommandsContainer.zrem(params[0], params[2]);
                break;
            case ZINCRBY:
                Double d = -Double.valueOf(params[1]);
                redisFuture =
                        this.redisCommandsContainer.zincrBy(params[0], d.toString(), params[2]);
                break;
            case HSET:
                {
                    redisFuture = this.redisCommandsContainer.hdel(params[0], params[1]);
                }
                break;
            case HINCRBY:
                redisFuture =
                        this.redisCommandsContainer.hincrBy(
                                params[0], params[1], -Long.valueOf(params[2]));
                break;
            case HINCRBYFLOAT:
                redisFuture =
                        this.redisCommandsContainer.hincrByFloat(
                                params[0], params[1], -Double.valueOf(params[2]));
                break;
            case INCRBY:
                redisFuture =
                        this.redisCommandsContainer.incrBy(params[0], -Long.valueOf(params[1]));
                break;
            case INCRBYFLOAT:
                redisFuture =
                        this.redisCommandsContainer.incrByFloat(
                                params[0], -Double.valueOf(params[1]));
                break;
            case DECRBY:
                redisFuture =
                        this.redisCommandsContainer.decrBy(params[0], -Long.valueOf(params[1]));
                break;
        }
        return redisFuture;
    }

    /**
     * set ttl for key.
     *
     * @param key
     */
    private void setTtl(String key) {
        if (redisCommand == RedisCommand.DEL) {
            return;
        }

        if (ttl != null) {
            if (ttlKeyNotAbsent) {
                // set ttl when key not absent
                this.redisCommandsContainer
                        .getTTL(key)
                        .whenComplete(
                                (t, h) -> {
                                    if (t < 0) {
                                        this.redisCommandsContainer.expire(key, ttl);
                                    }
                                });
            } else {
                // set ttl every sink
                this.redisCommandsContainer.expire(key, ttl);
            }
        } else if (expireTimeSeconds != -1) {
            this.redisCommandsContainer
                    .getTTL(key)
                    .whenComplete(
                            (t, h) -> {
                                if (t < 0) {
                                    int now = LocalTime.now().toSecondOfDay();
                                    this.redisCommandsContainer.expire(
                                            key,
                                            expireTimeSeconds > now
                                                    ? expireTimeSeconds - now
                                                    : 86400 + expireTimeSeconds - now);
                                }
                            });
        } else if (ttl != null) {
            this.redisCommandsContainer.expire(key, ttl);
        }
    }

    /**
     * serialize whole row.
     *
     * @param rowData
     * @return
     */
    private String serializeWholeRow(RowData rowData) {
        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < columnDataTypes.size(); i++) {
            stringBuilder.append(
                    RedisRowConverter.rowDataToString(
                            columnDataTypes.get(i).getLogicalType(), rowData, i));
            if (i != columnDataTypes.size() - 1) {
                stringBuilder.append(RedisDynamicTableFactory.CACHE_SEPERATOR);
            }
        }
        return stringBuilder.toString();
    }

    /**
     * calculate the number of redis command's param
     *
     * @return
     */
    private int calcParamNumByCommand() {
        if (redisCommand == RedisCommand.DEL) {
            return 1;
        }

        if (redisCommand.getRedisDataType() == RedisDataType.HASH
                || redisCommand.getRedisDataType() == RedisDataType.SORTED_SET) {
            if (redisCommand.getRedisOperationType() == RedisOperationType.INSERT
                    || redisCommand.getRedisOperationType() == RedisOperationType.ACC) {
                return 3;
            }
        }

        return 2;
    }

    /**
     * Initializes the connection to Redis by either cluster or sentinels or single server.
     *
     * @throws IllegalArgumentException if PoolConfig, ClusterConfig and SentinelConfig are all null
     */
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
