package org.apache.flink.streaming.connectors.redis.common.container;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisAdvancedClusterAsyncCommands;
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/** Redis command container if we want to connect to a Redis cluster. */
public class RedisClusterContainer implements RedisCommandsContainer, Closeable {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(RedisClusterContainer.class);

    protected transient RedisClusterClient redisClusterClient;

    protected transient StatefulRedisClusterConnection<String, String> connection;
    protected transient RedisAdvancedClusterAsyncCommands clusterAsyncCommands;
    protected transient RedisFuture redisFuture;

    /**
     * Initialize Redis command container for Redis cluster.
     *
     * @param redisClusterClient RedisClusterClient instance
     */
    public RedisClusterContainer(RedisClusterClient redisClusterClient) {
        Objects.requireNonNull(redisClusterClient, "redisClusterClient can not be null");
        this.redisClusterClient = redisClusterClient;
    }

    @Override
    public void open() throws Exception {
        connection = redisClusterClient.connect();
        clusterAsyncCommands = connection.async();
        LOG.info("open async connection!!!!");
    }

    /** Closes the {@link RedisClusterClient}. */
    @Override
    public void close() throws IOException {
        try {
            if (redisFuture != null) {
                redisFuture.await(2, TimeUnit.SECONDS);
            }
            this.connection.close();
        } catch (Exception e) {
            LOG.error("", e);
        }

        this.redisClusterClient.shutdown();
    }

    @Override
    public void hset(final String key, final String hashField, final String value) {
        try {
            redisFuture = clusterAsyncCommands.hset(key, hashField, value);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error(
                        "Cannot send Redis message with command HSET to hash {} of key {} error message {}",
                        hashField,
                        key,
                        e.getMessage());
            }
            throw e;
        }
    }

    @Override
    public void hincrBy(final String key, final String hashField, final long value) {
        try {
            redisFuture = clusterAsyncCommands.hincrby(key, hashField, value);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error(
                        "Cannot send Redis message with command HINCRBY to hash {} of key {} error message {}",
                        hashField,
                        key,
                        e.getMessage());
            }
            throw e;
        }
    }

    @Override
    public void hincrByFloat(final String key, final String hashField, final double value) {
        try {
            redisFuture = clusterAsyncCommands.hincrbyfloat(key, hashField, value);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error(
                        "Cannot send Redis message with command HINCRBY to hash {} of key {} error message {}",
                        hashField,
                        key,
                        e.getMessage());
            }
            throw e;
        }
    }

    @Override
    public void rpush(final String listName, final String value) {
        try {
            redisFuture = clusterAsyncCommands.rpush(listName, value);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error(
                        "Cannot send Redis message with command RPUSH to list {} error message: {}",
                        listName,
                        e.getMessage());
            }
            throw e;
        }
    }

    @Override
    public void lpush(String listName, String value) {
        try {
            redisFuture = clusterAsyncCommands.lpush(listName, value);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error(
                        "Cannot send Redis message with command LPUSH to list {} error message: {}",
                        listName,
                        e.getMessage());
            }
            throw e;
        }
    }

    @Override
    public void sadd(final String setName, final String value) {
        try {
            redisFuture = clusterAsyncCommands.sadd(setName, value);
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
            redisFuture = clusterAsyncCommands.publish(channelName, message);
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
            redisFuture = clusterAsyncCommands.set(key, value);
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
            redisFuture = clusterAsyncCommands.pfadd(key, element);
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
            redisFuture = clusterAsyncCommands.zadd(key, Double.valueOf(score), element);
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
            redisFuture = clusterAsyncCommands.zincrby(key, Double.valueOf(score), element);
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
            redisFuture = clusterAsyncCommands.zrem(key, element);
        } catch (Exception e) {
            if (LOG.isDebugEnabled()) {
                LOG.error(
                        "Cannot send Redis message with command ZREM to set {} error message {}",
                        key,
                        e.getMessage());
            }
        }
    }

    @Override
    public void incrBy(String key, long value) {
        try {
            redisFuture = clusterAsyncCommands.incrby(key, value);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error(
                        "Cannot send Redis message with command incrby to key {} with increment {} and tll {} error message {}",
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
            redisFuture = clusterAsyncCommands.incrbyfloat(key, value);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error(
                        "Cannot send Redis message with command incrby to key {} with increment {} and tll {} error message {}",
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
            redisFuture = clusterAsyncCommands.decrby(key, value);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error(
                        "Cannot send Redis message with command descry to key {} with decrement {} error message {}",
                        key,
                        value,
                        e.getMessage());
            }
            throw e;
        }
    }

    @Override
    public void hdel(String key, String field) {
        try {
            redisFuture = clusterAsyncCommands.hdel(key, field);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error(
                        "Cannot send Redis message with command hdel to key {} with field {} error message {}",
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
            redisFuture = clusterAsyncCommands.del(key);
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
            redisFuture = clusterAsyncCommands.expire(key, Duration.ofSeconds(seconds));
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
            redisFuture = clusterAsyncCommands.srem(setName, value);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error(
                        "Cannot send Redis message with command srem to setName {} with value {} error message {}",
                        setName,
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
            redisFuture = result = clusterAsyncCommands.hget(key, field);
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
    public RedisFuture<String> get(String key) {
        RedisFuture<String> result = null;
        try {
            redisFuture = result = clusterAsyncCommands.get(key);
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
    public RedisFuture<Map<String, String>> hgetAll(String key) {
        RedisFuture<Map<String, String>> result = null;
        try {
            result = redisFuture = clusterAsyncCommands.hgetall(key);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error(
                        "Cannot send Redis message with command hgetall to key {} error message {}",
                        key,
                        e.getMessage());
            }
            throw e;
        }
        return result;
    }

    @Override
    public RedisClusterAsyncCommands getAsyncCommands() {
        return clusterAsyncCommands;
    }

    @Override
    public RedisFuture<Long> getTTL(String key) {
        RedisFuture<Long> result = null;
        try {
            result = redisFuture = clusterAsyncCommands.ttl(key);
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
