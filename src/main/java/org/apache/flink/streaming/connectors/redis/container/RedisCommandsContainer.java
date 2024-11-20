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

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

/** The container for all available Redis commands. */
public interface RedisCommandsContainer extends Serializable {

    /**
     * Open the container.
     *
     * @throws Exception if the instance can not be opened properly
     */
    void open() throws Exception;

    /**
     * Sets field in the hash stored at key to value, with TTL, if needed. Setting expire time to
     * key is optional. If key does not exist, a new key holding a hash is created. If field already
     * exists in the hash, it is overwritten.
     *
     * @param key Hash name
     * @param hashField Hash field
     * @param value Hash value
     */
    RedisFuture<Boolean> hset(String key, String hashField, String value);

    /**
     * Sets fields in the hash stored at key to value, with TTL, if needed. Setting expire time to
     * key is optional. If key does not exist, a new key holding a hash is created. If key already
     * exists, it is overwritten.
     *
     * @param key
     * @param hashField
     * @return
     */
    RedisFuture<Boolean> hmset(String key, Map hashField);

    /**
     * @param key
     * @param hashField
     * @param value
     * @return
     */
    RedisFuture<Long> hincrBy(String key, String hashField, long value);

    /**
     * @param key
     * @param hashField
     * @param value
     * @return
     */
    RedisFuture<Double> hincrByFloat(String key, String hashField, double value);

    /**
     * Insert the specified value at the tail of the list stored at key. If key does not exist, it
     * is created as empty list before performing the push operation.
     *
     * @param listName Name of the List
     * @param value Value to be added
     */
    RedisFuture<Long> rpush(String listName, String value);

    /**
     * Insert the specified value at the head of the list stored at key. If key does not exist, it
     * is created as empty list before performing the push operation.
     *
     * @param listName Name of the List
     * @param value Value to be added
     */
    RedisFuture<Long> lpush(String listName, String value);

    /**
     * Add the specified member to the set stored at key. Specified members that are already a
     * member of this set are ignored. If key does not exist, a new set is created before adding the
     * specified members.
     *
     * @param setName Name of the Set
     * @param value Value to be added
     */
    RedisFuture<Long> sadd(String setName, String value);

    /**
     * Posts a message to the given channel.
     *
     * @param channelName Name of the channel to which data will be published
     * @param message the message
     */
    RedisFuture<Long> publish(String channelName, String message);

    /**
     * Set key to hold the string value. If key already holds a value, it is overwritten, regardless
     * of its type. Any previous time to live associated with the key is discarded on successful SET
     * operation.
     *
     * @param key the key name in which value to be set
     * @param value the value
     */
    RedisFuture<String> set(String key, String value);

    /**
     * Adds all the element arguments to the HyperLogLog data structure stored at the variable name
     * specified as first argument.
     *
     * @param key The name of the key
     * @param element the element
     */
    RedisFuture<Long> pfadd(String key, String element);

    /**
     * Adds the specified member with the specified scores to the sorted set stored at key.
     *
     * @param key The name of the Sorted Set
     * @param score Score of the element
     * @param element element to be added
     */
    RedisFuture<Long> zadd(String key, double score, String element);

    /**
     * increase the specified member with the specified scores to the sorted set stored at key.
     *
     * @param key
     * @param score
     * @param element
     */
    RedisFuture zincrBy(String key, double score, String element);

    /**
     * Removes the specified member from the sorted set stored at key.
     *
     * @param key The name of the Sorted Set
     * @param element element to be removed
     */
    RedisFuture<Long> zrem(String key, String element);

    /**
     * Remove members from a specified score range
     *
     * @param key
     * @param range
     * @return
     */
    RedisFuture<Long> zremRangeByScore(String key, Range<Double> range);

    /**
     * Remove members from a specified lex range
     *
     * @param key
     * @param range
     * @return
     */
    RedisFuture<Long> zremRangeByLex(String key, Range<String> range);

    /**
     * Remove members from a specified rank
     *
     * @param key
     * @param start
     * @param stop
     * @return
     */
    RedisFuture<Long> zremRangeByRank(String key, long start, long stop);

    /**
     * increase value to specified key.
     *
     * @param key
     * @param value
     */
    RedisFuture<Long> incrBy(String key, long value);

    /**
     * increase value to specified key.
     *
     * @param key
     * @param value
     * @return
     */
    RedisFuture<Double> incrByFloat(String key, double value);

    /**
     * decrease value from specified key.
     *
     * @param key the key name in which value to be set
     * @param value value the value
     */
    RedisFuture<Long> decrBy(String key, Long value);

    /**
     * get value by key and field .
     *
     * @param key
     * @param field
     * @return
     */
    RedisFuture<String> hget(String key, String field);

    /**
     * get all value by key.
     *
     * @param key
     * @return
     */
    RedisFuture<Map<String, String>> hgetAll(String key);

    /**
     * get value by key.
     *
     * @param key
     * @return
     */
    RedisFuture<String> get(String key);

    /**
     * Close the container.
     *
     * @throws IOException
     */
    void close() throws IOException;

    /**
     * expire key with seconds.
     *
     * @param key
     * @param seconds
     * @return
     */
    RedisFuture<Boolean> expire(String key, int seconds);

    /**
     * get ttl of key.
     *
     * @param key
     * @return
     */
    RedisFuture<Long> getTTL(String key);

    /**
     * delete key in map.
     *
     * @param key
     * @param field
     */
    RedisFuture<Long> hdel(String key, String field);

    /**
     * delete key.
     *
     * @param key
     */
    RedisFuture<Long> del(String key);

    /**
     * delete key value from set.
     *
     * @param setName
     * @param value
     */
    RedisFuture<Long> srem(String setName, String value);

    /**
     * @param key
     * @param start
     * @param end
     * @return
     */
    RedisFuture<List> lRange(String key, long start, long end);

    /**
     * @param key
     * @return
     */
    RedisFuture<Long> exists(String key);

    /**
     * @param key
     * @param field
     * @return
     */
    RedisFuture<Boolean> hexists(String key, String field);

    /**
     * @param key
     * @return
     */
    RedisFuture<Long> pfcount(String key);

    /**
     * @param key
     * @param member
     * @return
     */
    RedisFuture<Double> zscore(String key, String member);

    /**
     * @param key
     * @param start
     * @param stop
     * @return
     */
    RedisFuture<List> zrange(String key, long start, long stop);

    /**
     * @param key
     * @param count
     * @return
     */
    RedisFuture<List> srandmember(String key, long count);

    /**
     * @param key
     * @param xAddArgs
     * @param keysAndvalues
     * @return
     */
    RedisFuture<String> xadd(String key, XAddArgs xAddArgs, Object... keysAndvalues);

    /**
     * @param xReadArgs
     * @param streams
     * @return
     */
    RedisFuture<List<StreamMessage>> xread(
            XReadArgs xReadArgs, XReadArgs.StreamOffset<String>... streams);

    /**
     * @param key
     * @param range
     * @param limit
     * @return
     */
    RedisFuture<List<StreamMessage>> xrange(String key, Range<String> range, Limit limit);

    /**
     * @param key
     * @return
     */
    RedisFuture<Long> xlen(String key);
}
