/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.redis.container;

import io.lettuce.core.Limit;
import io.lettuce.core.Range;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XAddArgs;
import io.lettuce.core.XReadArgs;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisAdvancedClusterAsyncCommands;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Redis command container if we want to connect to a Redis cluster. */
public class RedisClusterContainer implements RedisCommandsContainer, Closeable {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(RedisClusterContainer.class);

    protected transient RedisClusterClient redisClusterClient;

    protected transient StatefulRedisClusterConnection<String, String> connection;
    protected transient RedisAdvancedClusterAsyncCommands clusterAsyncCommands;

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
    public void open() {
        connection = redisClusterClient.connect();
        clusterAsyncCommands = connection.async();
        LOG.info("open async connection!!!!");
    }

    /** Closes the {@link RedisClusterClient}. */
    @Override
    public void close() {
        this.connection.close();
        this.redisClusterClient.shutdown();
    }

    @Override
    public RedisFuture<Boolean> hset(final String key, final String hashField, final String value) {
        try {
            return clusterAsyncCommands.hset(key, hashField, value);
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
    public RedisFuture<Boolean> hmset(final String key, final Map hashField) {
        try {
            return clusterAsyncCommands.hmset(key, hashField);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error(
                        "Cannot send Redis message with command hmset to hash {} of key {} error message {}",
                        hashField,
                        key,
                        e.getMessage());
            }
            throw e;
        }
    }

    @Override
    public RedisFuture<Long> hincrBy(final String key, final String hashField, final long value) {
        try {
            return clusterAsyncCommands.hincrby(key, hashField, value);
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
    public RedisFuture<Double> hincrByFloat(
            final String key, final String hashField, final double value) {
        try {
            return clusterAsyncCommands.hincrbyfloat(key, hashField, value);
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
    public RedisFuture<Long> rpush(final String listName, final String value) {
        try {
            return clusterAsyncCommands.rpush(listName, value);
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
    public RedisFuture<Long> lpush(String listName, String value) {
        try {
            return clusterAsyncCommands.lpush(listName, value);
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
    public RedisFuture<Long> sadd(final String setName, final String value) {
        try {
            return clusterAsyncCommands.sadd(setName, value);
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
    public RedisFuture<Long> publish(final String channelName, final String message) {
        try {
            return clusterAsyncCommands.publish(channelName, message);
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
    public RedisFuture<String> set(final String key, final String value) {
        try {
            return clusterAsyncCommands.set(key, value);
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
    public RedisFuture<Long> pfadd(final String key, final String element) {
        try {
            return clusterAsyncCommands.pfadd(key, element);
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
    public RedisFuture<Long> zadd(final String key, final double score, final String element) {
        try {
            return clusterAsyncCommands.zadd(key, score, element);
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
    public RedisFuture<Double> zincrBy(final String key, final double score, final String element) {
        try {
            return clusterAsyncCommands.zincrby(key, score, element);
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
    public RedisFuture<Long> zrem(final String key, final String element) {
        try {
            return clusterAsyncCommands.zrem(key, element);
        } catch (Exception e) {
            if (LOG.isDebugEnabled()) {
                LOG.error(
                        "Cannot send Redis message with command ZREM to set {} error message {}",
                        key,
                        e.getMessage());
            }
            throw e;
        }
    }

    @Override
    public RedisFuture<Long> zremRangeByScore(String key, Range<Double> range) {
        try {
            return clusterAsyncCommands.zremrangebyscore(key, range);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error(
                        "Cannot send Redis message with command zremrangebyscore to set {} error message {}",
                        key,
                        e.getMessage());
            }
            throw e;
        }
    }

    @Override
    public RedisFuture<Long> zremRangeByLex(String key, Range<String> range) {
        try {
            return clusterAsyncCommands.zremrangebylex(key, range);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error(
                        "Cannot send Redis message with command zremrangebylex to set {} error message {}",
                        key,
                        e.getMessage());
            }
            throw e;
        }
    }

    @Override
    public RedisFuture<Long> zremRangeByRank(String key, long start, long stop) {
        try {
            return clusterAsyncCommands.zremrangebyrank(key, start, stop);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error(
                        "Cannot send Redis message with command zremrangebyrank to set {} error message {}",
                        key,
                        e.getMessage());
            }
            throw e;
        }
    }

    @Override
    public RedisFuture<Long> incrBy(String key, long value) {
        try {
            return clusterAsyncCommands.incrby(key, value);
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
    public RedisFuture<Double> incrByFloat(String key, double value) {
        try {
            return clusterAsyncCommands.incrbyfloat(key, value);
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
    public RedisFuture<Long> decrBy(String key, Long value) {
        try {
            return clusterAsyncCommands.decrby(key, value);
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
    public RedisFuture<Long> hdel(String key, String field) {
        try {
            return clusterAsyncCommands.hdel(key, field);
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
    public RedisFuture<Long> del(String key) {
        try {
            return clusterAsyncCommands.del(key);
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
    public RedisFuture<Boolean> expire(String key, int seconds) {
        try {
            return clusterAsyncCommands.expire(key, Duration.ofSeconds(seconds));
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
    public RedisFuture<Long> srem(String setName, String value) {
        try {
            return clusterAsyncCommands.srem(setName, value);
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
        try {
            return clusterAsyncCommands.hget(key, field);
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
    public RedisFuture<String> get(String key) {
        try {
            return clusterAsyncCommands.get(key);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error(
                        "Cannot send Redis message with command hget to key {} error message {}",
                        key,
                        e.getMessage());
            }
            throw e;
        }
    }

    @Override
    public RedisFuture<Map<String, String>> hgetAll(String key) {
        try {
            return clusterAsyncCommands.hgetall(key);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error(
                        "Cannot send Redis message with command hgetall to key {} error message {}",
                        key,
                        e.getMessage());
            }
            throw e;
        }
    }

    @Override
    public RedisFuture<Long> getTTL(String key) {
        try {
            return clusterAsyncCommands.ttl(key);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error(
                        "Cannot send Redis message with command ttl to key {} error message {}",
                        key,
                        e.getMessage());
            }
            throw e;
        }
    }

    @Override
    public RedisFuture<List> lRange(String key, long start, long end) {
        try {
            return clusterAsyncCommands.lrange(key, start, end);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error(
                        "Cannot send Redis message with command lrange to key {} start : {} end: {} error message {}",
                        key,
                        start,
                        end,
                        e.getMessage());
            }
            throw e;
        }
    }

    @Override
    public RedisFuture<Long> exists(String key) {
        try {
            return clusterAsyncCommands.exists(key);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error(
                        "Cannot send Redis message with command exists to key {} error message {}",
                        key,
                        e.getMessage());
            }
            throw e;
        }
    }

    @Override
    public RedisFuture<Boolean> hexists(String key, String field) {
        try {
            return clusterAsyncCommands.hexists(key, field);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error(
                        "Cannot send Redis message with command exists to key {} error message {}",
                        key,
                        e.getMessage());
            }
            throw e;
        }
    }

    @Override
    public RedisFuture<Long> pfcount(String key) {
        try {
            return clusterAsyncCommands.pfcount(key);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error(
                        "Cannot send Redis message with command pfcount to key {} error message {}",
                        key,
                        e.getMessage());
            }
            throw e;
        }
    }

    @Override
    public RedisFuture<Double> zscore(String key, String member) {
        try {
            return clusterAsyncCommands.zscore(key, member);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error(
                        "Cannot send Redis message with command zscore to key {} member {} error message {}",
                        key,
                        member,
                        e.getMessage());
            }
            throw e;
        }
    }

    @Override
    public RedisFuture<List> zrange(String key, long start, long stop) {
        try {
            return clusterAsyncCommands.zrange(key, start, stop);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error(
                        "Cannot send Redis message with command zrange to key {} start {} stop {} error message {}",
                        key,
                        start,
                        stop,
                        e.getMessage());
            }
            throw e;
        }
    }

    @Override
    public RedisFuture<List> srandmember(String key, long count) {
        try {
            return clusterAsyncCommands.srandmember(key, count);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error(
                        "Cannot send Redis message with command srandmember to key {} count {}error message {}",
                        key,
                        count,
                        e.getMessage());
            }
            throw e;
        }
    }

    @Override
    public RedisFuture<String> xadd(String key, XAddArgs xAddArgs, Object... keysAndvalues) {
        try {
            return clusterAsyncCommands.xadd(key, xAddArgs, keysAndvalues);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error(
                        "Cannot send Redis message with command xadd to key {} error message {}",
                        key,
                        e.getMessage());
            }
            throw e;
        }
    }

    @Override
    public RedisFuture<List<StreamMessage>> xread(
            XReadArgs xReadArgs, XReadArgs.StreamOffset<String>... streams) {
        try {
            return clusterAsyncCommands.xread(xReadArgs, streams);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error(
                        "Cannot send Redis message with command xread to key {} error message {}",
                        streams[0].getName(),
                        e.getMessage());
            }
            throw e;
        }
    }

    @Override
    public RedisFuture<List<StreamMessage>> xrange(String key, Range<String> range, Limit limit) {
        try {
            return clusterAsyncCommands.xrange(key, range, limit);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error(
                        "Cannot send Redis message with command xrange to key {} error message {}",
                        key,
                        e.getMessage());
            }
            throw e;
        }
    }

    @Override
    public RedisFuture<Long> xlen(String key) {
        try {
            return clusterAsyncCommands.xlen(key);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error(
                        "Cannot send Redis message with command xlen to key {} error message {}",
                        key,
                        e.getMessage());
            }
            throw e;
        }
    }
}
