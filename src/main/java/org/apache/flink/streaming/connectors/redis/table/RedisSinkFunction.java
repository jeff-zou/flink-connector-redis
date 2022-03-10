package org.apache.flink.streaming.connectors.redis.table;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisConfigBase;
import org.apache.flink.streaming.connectors.redis.common.container.RedisCommandsContainer;
import org.apache.flink.streaming.connectors.redis.common.container.RedisCommandsContainerBuilder;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisDataType;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisSinkMapper;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Objects;

public class RedisSinkFunction<IN> extends RichSinkFunction<IN> {

    private static final Logger LOG = LoggerFactory.getLogger(RedisSinkFunction.class);

    private boolean putIfAbsent;

    private Integer ttl;

    private RedisSinkMapper<IN> redisSinkMapper;
    private RedisCommand redisCommand;

    private FlinkJedisConfigBase flinkJedisConfigBase;
    private RedisCommandsContainer redisCommandsContainer;


    /**
     * Creates a new {@link RedisSinkFunction} that connects to the Redis server.
     *
     * @param flinkJedisConfigBase The configuration of {@link FlinkJedisConfigBase}
     * @param redisSinkMapper This is used to generate Redis command and key value from incoming elements.
     */
    public RedisSinkFunction(FlinkJedisConfigBase flinkJedisConfigBase, RedisSinkMapper<IN> redisSinkMapper, ResolvedSchema resolvedSchema) {
        Objects.requireNonNull(flinkJedisConfigBase, "Redis connection pool config should not be null");
        Objects.requireNonNull(redisSinkMapper, "Redis Mapper can not be null");
        Objects.requireNonNull(redisSinkMapper.getCommandDescription(), "Redis Mapper data type description can not be null");

        this.flinkJedisConfigBase = flinkJedisConfigBase;

        this.redisSinkMapper = redisSinkMapper;
        RedisCommandDescription redisCommandDescription = (RedisCommandDescription)redisSinkMapper.getCommandDescription();

        this.redisCommand = redisCommandDescription.getRedisCommand();
        this.putIfAbsent = redisCommandDescription.isPutIfAbsent();

        this.ttl = redisCommandDescription.getTTL();
    }



    /**
     * Called when new data arrives to the sink, and forwards it to Redis channel.
     * Depending on the specified Redis data type (see {@link RedisDataType}),
     * a different Redis command will be applied.
     * Available commands are RPUSH, LPUSH, SADD, PUBLISH, SET, SETEX, PFADD, HSET, ZADD.
     *
     * @param input The incoming data
     */
    @Override
    public void invoke(IN input, Context context) throws Exception {
        String key = redisSinkMapper.getKeyFromData(input, 0);
        String value = redisSinkMapper.getValueFromData(input, 1);
        String field = null;
        if(redisCommand.getRedisDataType() == RedisDataType.HASH || redisCommand.getRedisDataType() == RedisDataType.SORTED_SET){
            field = redisSinkMapper.getFieldFromData(input, 1);
            value = redisSinkMapper.getValueFromData(input, 2);
        }

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
            case SETEX:
                this.redisCommandsContainer.setex(key, value, this.ttl);
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
            case HSET:
                if(!putIfAbsent){
                    this.redisCommandsContainer.hset(key, field, value,this.ttl);
                } else if(!this.redisCommandsContainer.hexists(key, field)){
                    this.redisCommandsContainer.hset(key, field, value,this.ttl);
                }
                break;
            case HINCRBY:
                this.redisCommandsContainer.hincrBy(key, field, Long.valueOf(value), this.ttl);
                break;
            case INCRBY:
                this.redisCommandsContainer.incrBy(key, Long.valueOf(value));
                break;
            case INCRBY_EX:
                this.redisCommandsContainer.incrByEx(key, Long.valueOf(value), this.ttl);
                break;
            case DECRBY:
                this.redisCommandsContainer.decrBy(key, Long.valueOf(value));
                break;
            case DESCRBY_EX:
                this.redisCommandsContainer.decrByEx(key, Long.valueOf(value), this.ttl);
                break;
            default:
                throw new IllegalArgumentException("Cannot process such data type: " + redisCommand);
        }
    }

    /**
     * Initializes the connection to Redis by either cluster or sentinels or single server.
     *
     * @throws IllegalArgumentException if jedisPoolConfig, jedisClusterConfig and jedisSentinelConfig are all null
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        try {
            this.redisCommandsContainer = RedisCommandsContainerBuilder.build(this.flinkJedisConfigBase);
            this.redisCommandsContainer.open();
        } catch (Exception e) {
            LOG.error("Redis has not been properly initialized: ", e);
            throw e;
        }
    }

    /**
     * Closes commands container.
     * @throws IOException if command container is unable to close.
     */
    @Override
    public void close() throws IOException {
        if (redisCommandsContainer != null) {
            redisCommandsContainer.close();
        }
    }
}
