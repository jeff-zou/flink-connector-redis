package org.apache.flink.streaming.connectors.redis.common.mapper;

/** All available commands for Redis. Each command belongs to a {@link RedisDataType} group. */
public enum RedisCommand {

    /**
     * Insert the specified value at the head of the list stored at key. If key does not exist, it
     * is created as empty list before performing the push operations.
     */
    LPUSH(RedisDataType.LIST),

    /**
     * Insert the specified value at the tail of the list stored at key. If key does not exist, it
     * is created as empty list before performing the push operation.
     */
    RPUSH(RedisDataType.LIST),

    /**
     * Add the specified member to the set stored at key. Specified member that is already a member
     * of this set is ignored.
     */
    SADD(RedisDataType.SET),

    /**
     * Set key to hold the string value. If key already holds a value, it is overwritten, regardless
     * of its type.
     */
    SET(RedisDataType.STRING),

    /**
     * Set key to hold the string value, with a time to live (TTL). If key already holds a value, it
     * is overwritten, regardless of its type.
     */
    SETEX(RedisDataType.STRING),

    /**
     * Adds the element to the HyperLogLog data structure stored at the variable name specified as
     * first argument.
     */
    PFADD(RedisDataType.HYPER_LOG_LOG),

    /** Posts a message to the given channel. */
    PUBLISH(RedisDataType.PUBSUB),

    /** Adds the specified members with the specified score to the sorted set stored at key. */
    ZADD(RedisDataType.SORTED_SET),

    ZINCRBY(RedisDataType.SORTED_SET),

    /** Removes the specified members from the sorted set stored at key. */
    ZREM(RedisDataType.SORTED_SET),

    /**
     * Sets field in the hash stored at key to value. If key does not exist, a new key holding a
     * hash is created. If field already exists in the hash, it is overwritten.
     */
    HSET(RedisDataType.HASH),

    HINCRBY(RedisDataType.HINCRBY),

    /** Delta plus for specified key. */
    INCRBY(RedisDataType.STRING),

    /** Delta plus for specified key and expire the key with fixed time. */
    INCRBY_EX(RedisDataType.STRING),

    /** decrease with fixed num for specified key. */
    DECRBY(RedisDataType.STRING),

    /** decrease with fixed num for specified key and expire the key with fixed time. */
    DESCRBY_EX(RedisDataType.STRING),

    /** get val from map. */
    HGET(RedisDataType.HASH),

    /** del val in map. */
    HDEL(RedisDataType.HASH),

    /** del key. */
    DEL(RedisDataType.SET),

    /** get val from string. */
    GET(RedisDataType.STRING);

    /** The {@link RedisDataType} this command belongs to. */
    private RedisDataType redisDataType;

    RedisCommand(RedisDataType redisDataType) {
        this.redisDataType = redisDataType;
    }

    /**
     * The {@link RedisDataType} this command belongs to.
     *
     * @return the {@link RedisDataType}
     */
    public RedisDataType getRedisDataType() {
        return redisDataType;
    }
}
