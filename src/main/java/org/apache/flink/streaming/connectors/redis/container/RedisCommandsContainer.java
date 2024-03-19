package org.apache.flink.streaming.connectors.redis.container;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;

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
    RedisFuture<Long> zadd(String key, String score, String element);

    /**
     * increase the specified member with the specified scores to the sorted set stored at key.
     *
     * @param key
     * @param score
     * @param element
     */
    RedisFuture zincrBy(String key, String score, String element);

    /**
     * Removes the specified member from the sorted set stored at key.
     *
     * @param key The name of the Sorted Set
     * @param element element to be removed
     */
    RedisFuture<Long> zrem(String key, String element);

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
     * get redis async commands.
     *
     * @return
     */
    RedisClusterAsyncCommands getAsyncCommands();
}
