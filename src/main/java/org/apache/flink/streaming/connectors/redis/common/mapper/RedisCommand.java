package org.apache.flink.streaming.connectors.redis.common.mapper;

/** All available commands for Redis. Each command belongs to a {@link RedisDataType} group. */
public enum RedisCommand {

    /**
     * Insert the specified value at the head of the list stored at key. If key does not exist, it
     * is created as empty list before performing the push operations.
     */
    LPUSH(RedisQueryCommand.LRANGE, RedisSinkCommand.LPUSH, RedisDelCommand.LREM, true),

    /**
     * Insert the specified value at the tail of the list stored at key. If key does not exist, it
     * is created as empty list before performing the push operation.
     */
    RPUSH(RedisQueryCommand.LRANGE, RedisSinkCommand.RPUSH, RedisDelCommand.LREM, true),

    /**
     * Add the specified member to the set stored at key. Specified member that is already a member
     * of this set is ignored.
     */
    SADD(RedisQueryCommand.SMEMBERS, RedisSinkCommand.SADD, RedisDelCommand.SREM, true),

    /**
     * Set key to hold the string value. If key already holds a value, it is overwritten, regardless
     * of its type.
     */
    SET(RedisQueryCommand.GET, RedisSinkCommand.SET, RedisDelCommand.DEL, true),

    /**
     * Adds the element to the HyperLogLog data structure stored at the variable name specified as
     * first argument.
     */
    PFADD(RedisQueryCommand.PFCOUNT, RedisSinkCommand.PFADD, RedisDelCommand.NONE, true),

    /** Posts a message to the given channel. */
    PUBLISH(RedisQueryCommand.SUBSCRIBE, RedisSinkCommand.PUBLISH, RedisDelCommand.NONE, false),

    /** Adds the specified members with the specified score to the sorted set stored at key. */
    ZADD(RedisQueryCommand.ZRANGE, RedisSinkCommand.ZADD, RedisDelCommand.ZREM, true),

    /** */
    ZINCRBY(RedisQueryCommand.ZSCORE, RedisSinkCommand.ZINCRBY, RedisDelCommand.ZINCRBY, true),

    /** Removes the specified members from the sorted set stored at key. */
    ZREM(RedisQueryCommand.ZRANGE, RedisSinkCommand.ZREM, RedisDelCommand.ZREM, true),

    /** Removes the specified members from set at key. */
    SREM(RedisQueryCommand.ZRANGE, RedisSinkCommand.SREM, RedisDelCommand.SREM, true),

    /**
     * Sets field in the hash stored at key to value. If key does not exist, a new key holding a
     * hash is created. If field already exists in the hash, it is overwritten.
     */
    HSET(RedisQueryCommand.HGET, RedisSinkCommand.HSET, RedisDelCommand.HDEL, true),

    /** Delta plus for specified key. */
    HINCRBY(RedisQueryCommand.HGET, RedisSinkCommand.HINCRBY, RedisDelCommand.HINCRBY, true),

    /** Delta plus for specified key. */
    HINCRBYFLOAT(
            RedisQueryCommand.HGET,
            RedisSinkCommand.HINCRBYFLOAT,
            RedisDelCommand.HINCRBYFLOAT,
            true),

    /** Delta plus for specified key. */
    INCRBY(RedisQueryCommand.GET, RedisSinkCommand.INCRBY, RedisDelCommand.INCRBY, true),

    /** Delta plus for specified key. */
    INCRBYFLOAT(
            RedisQueryCommand.GET, RedisSinkCommand.INCRBYFLOAT, RedisDelCommand.INCRBYFLOAT, true),

    /** decrease with fixed num for specified key. */
    DECRBY(RedisQueryCommand.GET, RedisSinkCommand.DECRBY, RedisDelCommand.DECRBY, true),

    /** get val from map. */
    HGET(RedisQueryCommand.HGET, RedisSinkCommand.HSET, RedisDelCommand.HDEL, true),

    /** del val in map. */
    HDEL(RedisQueryCommand.HGET, RedisSinkCommand.HDEL, RedisDelCommand.HDEL, true),

    /** del key. */
    DEL(RedisQueryCommand.GET, RedisSinkCommand.DEL, RedisDelCommand.DEL, true),

    /** get val from string. */
    GET(RedisQueryCommand.GET, RedisSinkCommand.SET, RedisDelCommand.DEL, true);

    /** The {@link RedisDataType} this command belongs to. */
    private RedisQueryCommand queryCommand;

    private RedisSinkCommand sinkCommand;

    private RedisDelCommand delCommand;

    private boolean commandBoundedness;

    RedisCommand(
            RedisQueryCommand queryCommand,
            RedisSinkCommand sinkCommand,
            RedisDelCommand delCommand,
            boolean commandBoundedness) {
        this.queryCommand = queryCommand;
        this.sinkCommand = sinkCommand;
        this.delCommand = delCommand;
        this.commandBoundedness = commandBoundedness;
    }

    public RedisQueryCommand getQueryCommand() {
        return queryCommand;
    }

    public RedisSinkCommand getSinkCommand() {
        return sinkCommand;
    }

    public RedisDelCommand getDelCommand() {
        return delCommand;
    }

    public boolean isCommandBoundedness() {
        return commandBoundedness;
    }
}
