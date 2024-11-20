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
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XAddArgs;
import io.lettuce.core.XReadArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.List;
import java.util.Map;

/**
 * Redis command container if we want to connect to a single Redis server or to Redis sentinels If
 * want to connect to a single Redis server, please use the first constructor {@link
 * #RedisContainer(RedisClient)}. If want to connect to a Redis sentinels or single
 */
public class RedisContainer implements RedisCommandsContainer, Closeable {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(RedisContainer.class);
    private final transient RedisClient redisClient;
    protected transient StatefulRedisConnection<String, String> connection;
    protected transient RedisAsyncCommands asyncCommands;

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
    public void close() {
        this.connection.close();
        this.redisClient.shutdown();
    }

    @Override
    public void open() {
        connection = redisClient.connect();
        asyncCommands = connection.async();
        LOG.info("open async connection!!!!");
    }

    @Override
    public RedisFuture<Boolean> hset(final String key, final String hashField, final String value) {
        try {
            return asyncCommands.hset(key, hashField, value);
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
    public RedisFuture<Boolean> hmset(final String key, final Map hashField) {
        try {
            return asyncCommands.hmset(key, hashField);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error(
                        "Cannot send Redis message with command hmset to key {} and hashField {} error message {}",
                        key,
                        hashField,
                        e.getMessage());
            }
            throw e;
        }
    }

    @Override
    public RedisFuture<Long> hincrBy(final String key, final String hashField, final long value) {
        try {
            return asyncCommands.hincrby(key, hashField, value);
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
    public RedisFuture<Double> hincrByFloat(
            final String key, final String hashField, final double value) {
        try {
            return asyncCommands.hincrbyfloat(key, hashField, value);
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
    public RedisFuture<Long> rpush(final String listName, final String value) {
        try {
            return asyncCommands.rpush(listName, value);
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
    public RedisFuture<Long> lpush(String listName, String value) {
        try {
            return asyncCommands.lpush(listName, value);
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
    public RedisFuture<Long> sadd(final String setName, final String value) {

        try {
            return asyncCommands.sadd(setName, value);
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
            return asyncCommands.publish(channelName, message);
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
            return asyncCommands.set(key, value);
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
            return asyncCommands.pfadd(key, element);
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
            return asyncCommands.zadd(key, score, element);
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
            return asyncCommands.zincrby(key, score, element);
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
            return asyncCommands.zrem(key, element);
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
    public RedisFuture<Long> zremRangeByScore(String key, Range<Double> range) {
        try {
            return asyncCommands.zremrangebyscore(key, range);
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
            return asyncCommands.zremrangebylex(key, range);
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
            return asyncCommands.zremrangebyrank(key, start, stop);
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
            return asyncCommands.incrby(key, value);
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
    public RedisFuture<Double> incrByFloat(String key, double value) {
        try {
            return asyncCommands.incrbyfloat(key, value);
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
    public RedisFuture<Long> decrBy(String key, Long value) {
        try {
            return asyncCommands.decrby(key, value);
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
        try {
            return asyncCommands.hget(key, field);
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
    public RedisFuture<Map<String, String>> hgetAll(String key) {
        try {
            return asyncCommands.hgetall(key);
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
    public RedisFuture<String> get(String key) {
        try {
            return asyncCommands.get(key);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error(
                        "Cannot send Redis message with command get to key {} error message {}",
                        key,
                        e.getMessage());
            }
            throw e;
        }
    }

    @Override
    public RedisFuture<Long> hdel(String key, String field) {
        try {
            return asyncCommands.hdel(key, field);
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
    public RedisFuture<Long> del(String key) {
        try {
            return asyncCommands.del(key);
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
            return asyncCommands.expire(key, seconds);
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
            return asyncCommands.srem(setName, value);
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
    public RedisFuture<Long> getTTL(String key) {
        try {
            return asyncCommands.ttl(key);
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
            return asyncCommands.lrange(key, start, end);
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
            return asyncCommands.exists(key);
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
            return asyncCommands.hexists(key, field);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error(
                        "Cannot send Redis message with command hexists to key {} field {} error message {}",
                        key,
                        field,
                        e.getMessage());
            }
            throw e;
        }
    }

    @Override
    public RedisFuture<Long> pfcount(String key) {
        try {
            return asyncCommands.pfcount(key);
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
            return asyncCommands.zscore(key, member);
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
            return asyncCommands.zrange(key, start, stop);
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
            return asyncCommands.srandmember(key, count);
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
            return asyncCommands.xadd(key, xAddArgs, keysAndvalues);
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
            return asyncCommands.xread(xReadArgs, streams);
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
            return asyncCommands.xrange(key, range, limit);
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
            return asyncCommands.xlen(key);
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
