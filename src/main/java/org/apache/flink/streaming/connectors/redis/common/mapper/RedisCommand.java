package org.apache.flink.streaming.connectors.redis.common.mapper;

/** All available commands for Redis. Each command belongs to a {@link RedisDataType} group. */
public enum RedisCommand {

    /**
     * Insert the specified value at the head of the list stored at key. If key does not exist, it
     * is created as empty list before performing the push operations.
     */
    LPUSH(RedisDataType.LIST, RedisOperationType.INSERT),

    /**
     * Insert the specified value at the tail of the list stored at key. If key does not exist, it
     * is created as empty list before performing the push operation.
     */
    RPUSH(RedisDataType.LIST, RedisOperationType.INSERT),

    /**
     * Add the specified member to the set stored at key. Specified member that is already a member
     * of this set is ignored.
     */
    SADD(RedisDataType.SET, RedisOperationType.INSERT),

    /**
     * Set key to hold the string value. If key already holds a value, it is overwritten, regardless
     * of its type.
     */
    SET(RedisDataType.STRING, RedisOperationType.INSERT),

    /**
     * Set key to hold the string value, with a time to live (TTL). If key already holds a value, it
     * is overwritten, regardless of its type.
     */
    SETEX(RedisDataType.STRING, RedisOperationType.INSERT),

    /**
     * Adds the element to the HyperLogLog data structure stored at the variable name specified as
     * first argument.
     */
    PFADD(RedisDataType.HYPER_LOG_LOG, RedisOperationType.INSERT),

    /** Posts a message to the given channel. */
    PUBLISH(RedisDataType.PUBSUB, RedisOperationType.INSERT),

    /** Adds the specified members with the specified score to the sorted set stored at key. */
    ZADD(RedisDataType.SORTED_SET, RedisOperationType.INSERT),

    /** */
    ZINCRBY(RedisDataType.SORTED_SET, RedisOperationType.ACC),

    /** Removes the specified members from the sorted set stored at key. */
    ZREM(RedisDataType.SORTED_SET, RedisOperationType.DEL),

    /** Removes the specified members from set at key. */
    SREM(RedisDataType.SET, RedisOperationType.DEL),

    /**
     * Sets field in the hash stored at key to value. If key does not exist, a new key holding a
     * hash is created. If field already exists in the hash, it is overwritten.
     */
    HSET(RedisDataType.HASH, RedisOperationType.INSERT),

    /** Delta plus for specified key. */
    HINCRBY(RedisDataType.HASH, RedisOperationType.ACC),

    /** Delta plus for specified key. */
    HINCRBYFLOAT(RedisDataType.HASH, RedisOperationType.ACC),

    /** Delta plus for specified key. */
    INCRBY(RedisDataType.STRING, RedisOperationType.ACC),

    /** Delta plus for specified key. */
    INCRBYFLOAT(RedisDataType.STRING, RedisOperationType.ACC),

    /** Delta plus for specified key and expire the key with fixed time. */
    INCRBY_EX(RedisDataType.STRING, RedisOperationType.ACC),

    /** decrease with fixed num for specified key. */
    DECRBY(RedisDataType.STRING, RedisOperationType.ACC),

    /** decrease with fixed num for specified key and expire the key with fixed time. */
    DESCRBY_EX(RedisDataType.STRING, RedisOperationType.ACC),

    /** get val from map. */
    HGET(RedisDataType.HASH, RedisOperationType.QUERY),

    /** del val in map. */
    HDEL(RedisDataType.HASH, RedisOperationType.DEL),

    /** del key. */
    DEL(RedisDataType.STRING, RedisOperationType.DEL),

    /** get val from string. */
    GET(RedisDataType.STRING, RedisOperationType.QUERY);

    /** The {@link RedisDataType} this command belongs to. */
    private RedisDataType redisDataType;

    private RedisOperationType redisOperationType;

    RedisCommand(RedisDataType redisDataType, RedisOperationType redisOperationType) {
        this.redisDataType = redisDataType;
        this.redisOperationType = redisOperationType;
    }

    /**
     * The {@link RedisDataType} this command belongs to.
     *
     * @return the {@link RedisDataType}
     */
    public RedisDataType getRedisDataType() {
        return redisDataType;
    }

    /**
     * The {@link RedisOperationType} this command belongs to.
     *
     * @return the {@link RedisOperationType}
     */
    public RedisOperationType getRedisOperationType() {
        return redisOperationType;
    }
}
