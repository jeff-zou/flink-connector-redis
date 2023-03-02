package org.apache.flink.streaming.connectors.redis.common.container;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Redis command container if we want to connect to a single Redis server or to Redis sentinels If
 * want to connect to a single Redis server, please use the first constructor {@link
 * #RedisContainer(RedisClient)}. If want to connect to a Redis sentinels or single
 */
public class RedisContainer implements RedisCommandsContainer, Closeable {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(RedisContainer.class);

    private transient RedisClient redisClient;
    protected transient StatefulRedisConnection<String, String> connection;
    protected transient RedisAsyncCommands asyncCommands;
    private transient RedisFuture redisFuture;

    /**
     * Use this constructor if to connect with single Redis server.
     *
     * @param redisClient
     */
    public RedisContainer(RedisClient redisClient) {
        this.redisClient = redisClient;
    }

    /** Closes the redisClient instances. */
    @Override
    public void close() throws IOException {
        try {
            if (redisFuture != null) {
                redisFuture.await(2, TimeUnit.SECONDS);
            }
            connection.close();
        } catch (Exception e) {
            LOG.info("", e);
        }
        redisClient.shutdown();
    }

    @Override
    public void open() throws Exception {
        connection = redisClient.connect();
        asyncCommands = connection.async();
        LOG.info("open async connection!!!!");
    }

    @Override
    public void hset(final String key, final String hashField, final String value) {

        try {
            redisFuture = asyncCommands.hset(key, hashField, value);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error(
                        "Cannot send Redis message with command HSET to key {} and hashField {} error message {}",
                        key,
                        hashField,
                        e.getMessage());
            }
            throw e;
        }
    }

    @Override
    public void hincrBy(final String key, final String hashField, final long value) {
        try {
            redisFuture = asyncCommands.hincrby(key, hashField, value);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error(
                        "Cannot send Redis message with command HINCRBY to key {} and hashField {} error message {}",
                        key,
                        hashField,
                        e.getMessage());
            }
            throw e;
        }
    }

    @Override
    public void hincrByFloat(final String key, final String hashField, final double value) {
        try {
            redisFuture = asyncCommands.hincrbyfloat(key, hashField, value);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error(
                        "Cannot send Redis message with command HINCRBY to key {} and hashField {} error message {}",
                        key,
                        hashField,
                        e.getMessage());
            }
            throw e;
        }
    }

    @Override
    public void rpush(final String listName, final String value) {
        try {
            redisFuture = asyncCommands.rpush(listName, value);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error(
                        "Cannot send Redis message with command RPUSH to list {} error message {}",
                        listName,
                        e.getMessage());
            }
            throw e;
        }
    }

    @Override
    public void lpush(String listName, String value) {
        try {
            redisFuture = asyncCommands.lpush(listName, value);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error(
                        "Cannot send Redis message with command LUSH to list {} error message {}",
                        listName,
                        e.getMessage());
            }
            throw e;
        }
    }

    @Override
    public void sadd(final String setName, final String value) {

        try {
            redisFuture = asyncCommands.sadd(setName, value);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error(
                        "Cannot send Redis message with command RPUSH to set {} error message {}",
                        setName,
                        e.getMessage());
            }
            throw e;
        }
    }

    @Override
    public void publish(final String channelName, final String message) {

        try {
            redisFuture = asyncCommands.publish(channelName, message);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error(
                        "Cannot send Redis message with command PUBLISH to channel {} error message {}",
                        channelName,
                        e.getMessage());
            }
            throw e;
        }
    }

    @Override
    public void set(final String key, final String value) {

        try {
            redisFuture = asyncCommands.set(key, value);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error(
                        "Cannot send Redis message with command SET to key {} error message {}",
                        key,
                        e.getMessage());
            }
            throw e;
        }
    }

    @Override
    public void pfadd(final String key, final String element) {

        try {
            redisFuture = asyncCommands.pfadd(key, element);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error(
                        "Cannot send Redis message with command PFADD to key {} error message {}",
                        key,
                        e.getMessage());
            }
            throw e;
        }
    }

    @Override
    public void zadd(final String key, final String score, final String element) {

        try {
            redisFuture = asyncCommands.zadd(key, Double.valueOf(score), element);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error(
                        "Cannot send Redis message with command ZADD to set {} error message {}",
                        key,
                        e.getMessage());
            }
            throw e;
        }
    }

    @Override
    public void zincrBy(final String key, final String score, final String element) {

        try {
            redisFuture = asyncCommands.zincrby(key, Double.valueOf(score), element);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error(
                        "Cannot send Redis message with command ZINCRBY to set {} error message {}",
                        key,
                        e.getMessage());
            }
            throw e;
        }
    }

    @Override
    public void zrem(final String key, final String element) {

        try {
            redisFuture = asyncCommands.zrem(key, element);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error(
                        "Cannot send Redis message with command ZREM to set {} error message {}",
                        key,
                        e.getMessage());
            }
            throw e;
        }
    }

    @Override
    public void incrBy(String key, long value) {

        try {
            redisFuture = asyncCommands.incrby(key, value);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error(
                        "Cannot send Redis with incrby command with increment {}  error message {}",
                        key,
                        value,
                        e.getMessage());
            }
            throw e;
        }
    }

    @Override
    public void incrByFloat(String key, double value) {

        try {
            redisFuture = asyncCommands.incrbyfloat(key, value);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error(
                        "Cannot send Redis with incrby command with increment {}  error message {}",
                        key,
                        value,
                        e.getMessage());
            }
            throw e;
        }
    }

    @Override
    public void decrBy(String key, Long value) {

        try {
            redisFuture = asyncCommands.decrby(key, value);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error(
                        "Cannot send Redis with decrBy command with increment {}  error message {}",
                        key,
                        value,
                        e.getMessage());
            }
            throw e;
        }
    }

    @Override
    public RedisFuture<String> hget(String key, String field) {

        RedisFuture<String> result = null;
        try {
            redisFuture = result = asyncCommands.hget(key, field);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error(
                        "Cannot send Redis message with command hget to key {} with field {} error message {}",
                        key,
                        field,
                        e.getMessage());
            }
            throw e;
        }
        return result;
    }

    @Override
    public RedisFuture<Map<String, String>> hgetAll(String key) {

        RedisFuture<Map<String, String>> result = null;
        try {
            redisFuture = result = asyncCommands.hgetall(key);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error(
                        "Cannot send Redis message with command hget to key {} error message {}",
                        key,
                        e.getMessage());
            }
            throw e;
        }
        return result;
    }

    @Override
    public RedisFuture<String> get(String key) {

        RedisFuture<String> result = null;
        try {
            redisFuture = result = asyncCommands.get(key);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error(
                        "Cannot send Redis message with command get to key {} error message {}",
                        key,
                        e.getMessage());
            }
            throw e;
        }
        return result;
    }

    @Override
    public void hdel(String key, String field) {

        try {
            redisFuture = asyncCommands.hdel(key, field);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error(
                        "Cannot send Redis message with command hget to key {} with field {} error message {}",
                        key,
                        field,
                        e.getMessage());
            }
            throw e;
        }
    }

    @Override
    public void del(String key) {

        try {
            redisFuture = asyncCommands.hdel(key);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error(
                        "Cannot send Redis message with command del to key {} error message {}",
                        key,
                        e.getMessage());
            }
            throw e;
        }
    }

    @Override
    public void expire(String key, int seconds) {

        try {
            redisFuture = asyncCommands.expire(key, seconds);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error(
                        "Cannot send Redis message with command exists to key {}  seconds {} error message {}",
                        key,
                        seconds,
                        e.getMessage());
            }
            throw e;
        }
    }

    @Override
    public void srem(String setName, String value) {

        try {
            redisFuture = asyncCommands.srem(setName, value);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error(
                        "Cannot send Redis message with command srem to setName {} value : {} error message {}",
                        setName,
                        value,
                        e.getMessage());
            }
            throw e;
        }
    }

    @Override
    public RedisClusterAsyncCommands getAsyncCommands() {
        return asyncCommands;
    }

    @Override
    public RedisFuture<Long> getTTL(String key) {
        RedisFuture<Long> result = null;
        try {
            result = redisFuture = asyncCommands.ttl(key);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error(
                        "Cannot send Redis message with command ttl to key {} error message {}",
                        key,
                        e.getMessage());
            }
            throw e;
        }
        return result;
    }
}
