package org.apache.flink.streaming.connectors.redis.command;

import org.apache.flink.streaming.connectors.redis.mapper.RedisDataType;

/** All available commands for Redis. Each command belongs to a {@link RedisDataType} group. */
public enum RedisCommand {

    /**
     * Set key to hold the string value. If key already holds a value, it is overwritten, regardless
     * of its type.
     */
    SET(
            RedisInsertCommand.SET,
            RedisSelectCommand.GET,
            RedisJoinCommand.GET,
            RedisDeleteCommand.DEL,
            true),

    /**
     * Sets field in the hash stored at key to value. If key does not exist, a new key holding a
     * hash is created. If field already exists in the hash, it is overwritten.
     */
    HSET(
            RedisInsertCommand.HSET,
            RedisSelectCommand.HGET,
            RedisJoinCommand.HGET,
            RedisDeleteCommand.HDEL,
            true),

    HMSET(
            RedisInsertCommand.HMSET,
            RedisSelectCommand.HGET,
            RedisJoinCommand.HGET,
            RedisDeleteCommand.HDEL,
            true),

    /** get val from map. */
    HGET(
            RedisInsertCommand.HSET,
            RedisSelectCommand.HGET,
            RedisJoinCommand.HGET,
            RedisDeleteCommand.HDEL,
            true),

    /** get val from string. */
    GET(
            RedisInsertCommand.SET,
            RedisSelectCommand.GET,
            RedisJoinCommand.GET,
            RedisDeleteCommand.DEL,
            true),

    /**
     * Insert the specified value at the tail of the list stored at key. If key does not exist, it
     * is created as empty list before performing the push operation.
     */
    RPUSH(
            RedisInsertCommand.RPUSH,
            RedisSelectCommand.LRANGE,
            RedisJoinCommand.NONE,
            RedisDeleteCommand.NONE,
            true),
    /**
     * Insert the specified value at the head of the list stored at key. If key does not exist, it
     * is created as empty list before performing the push operations.
     */
    LPUSH(
            RedisInsertCommand.LPUSH,
            RedisSelectCommand.LRANGE,
            RedisJoinCommand.NONE,
            RedisDeleteCommand.NONE,
            true),

    /** Delta plus for specified key. */
    INCRBY(
            RedisInsertCommand.INCRBY,
            RedisSelectCommand.GET,
            RedisJoinCommand.GET,
            RedisDeleteCommand.INCRBY,
            true),

    /** Delta plus for specified key. */
    INCRBYFLOAT(
            RedisInsertCommand.INCRBYFLOAT,
            RedisSelectCommand.GET,
            RedisJoinCommand.GET,
            RedisDeleteCommand.INCRBYFLOAT,
            true),

    /** Delta plus for specified key. */
    HINCRBY(
            RedisInsertCommand.HINCRBY,
            RedisSelectCommand.HGET,
            RedisJoinCommand.HGET,
            RedisDeleteCommand.HINCRBY,
            true),

    /** Delta plus for specified key. */
    HINCRBYFLOAT(
            RedisInsertCommand.HINCRBYFLOAT,
            RedisSelectCommand.HGET,
            RedisJoinCommand.HGET,
            RedisDeleteCommand.HINCRBYFLOAT,
            true),

    /** */
    ZINCRBY(
            RedisInsertCommand.ZINCRBY,
            RedisSelectCommand.ZSCORE,
            RedisJoinCommand.ZSCORE,
            RedisDeleteCommand.ZINCRBY,
            true),

    /**
     * Add the specified member to the set stored at key. Specified member that is already a member
     * of this set is ignored.
     */
    SADD(
            RedisInsertCommand.SADD,
            RedisSelectCommand.SRANDMEMBER,
            RedisJoinCommand.NONE,
            RedisDeleteCommand.SREM,
            true),

    /** Adds the specified members with the specified score to the sorted set stored at key. */
    ZADD(
            RedisInsertCommand.ZADD,
            RedisSelectCommand.ZSCORE,
            RedisJoinCommand.ZSCORE,
            RedisDeleteCommand.ZREM,
            true),

    /**
     * Adds the element to the HyperLogLog data structure stored at the variable name specified as
     * first argument.
     */
    PFADD(
            RedisInsertCommand.PFADD,
            RedisSelectCommand.NONE,
            RedisJoinCommand.NONE,
            RedisDeleteCommand.NONE,
            true),

    /** Posts a message to the given channel. */
    PUBLISH(
            RedisInsertCommand.PUBLISH,
            RedisSelectCommand.SUBSCRIBE,
            RedisJoinCommand.NONE,
            RedisDeleteCommand.NONE,
            false),

    /** Posts a message to the given channel. */
    SUBSCRIBE(
            RedisInsertCommand.PUBLISH,
            RedisSelectCommand.SUBSCRIBE,
            RedisJoinCommand.NONE,
            RedisDeleteCommand.NONE,
            false),

    /** Removes the specified members from set at key. */
    SREM(
            RedisInsertCommand.SREM,
            RedisSelectCommand.SRANDMEMBER,
            RedisJoinCommand.NONE,
            RedisDeleteCommand.NONE,
            true),

    /** Removes the specified members from the sorted set stored at key. */
    ZREM(
            RedisInsertCommand.ZREM,
            RedisSelectCommand.ZSCORE,
            RedisJoinCommand.ZSCORE,
            RedisDeleteCommand.NONE,
            true),

    /** del key. */
    DEL(
            RedisInsertCommand.DEL,
            RedisSelectCommand.GET,
            RedisJoinCommand.GET,
            RedisDeleteCommand.NONE,
            true),

    /** del val in map. */
    HDEL(
            RedisInsertCommand.HDEL,
            RedisSelectCommand.HGET,
            RedisJoinCommand.HGET,
            RedisDeleteCommand.NONE,
            true),
    /** decrease with fixed num for specified key. */
    DECRBY(
            RedisInsertCommand.DECRBY,
            RedisSelectCommand.GET,
            RedisJoinCommand.GET,
            RedisDeleteCommand.NONE,
            true);

    /** The {@link RedisDataType} this command belongs to. */
    private RedisSelectCommand selectCommand;

    private RedisInsertCommand insertCommand;

    private RedisDeleteCommand deleteCommand;

    private RedisJoinCommand joinCommand;

    private boolean commandBoundedness;

    RedisCommand(
            RedisInsertCommand insertCommand,
            RedisSelectCommand selectCommand,
            RedisJoinCommand joinCommand,
            RedisDeleteCommand deleteCommand,
            boolean commandBoundedness) {
        this.selectCommand = selectCommand;
        this.insertCommand = insertCommand;
        this.deleteCommand = deleteCommand;
        this.joinCommand = joinCommand;
        this.commandBoundedness = commandBoundedness;
    }

    public RedisSelectCommand getSelectCommand() {
        return selectCommand;
    }

    public RedisInsertCommand getInsertCommand() {
        return insertCommand;
    }

    public RedisDeleteCommand getDeleteCommand() {
        return deleteCommand;
    }

    public RedisJoinCommand getJoinCommand() {
        return joinCommand;
    }

    public boolean isCommandBoundedness() {
        return commandBoundedness;
    }
}
