package org.apache.flink.streaming.connectors.redis.command;

public enum RedisInsertCommand {
    RPUSH,
    LPUSH,
    SADD,
    PFADD,
    SET,
    PUBLISH,
    ZADD,
    SREM,
    ZREM,
    ZINCRBY,
    HSET,
    HMSET,
    HINCRBY,
    HINCRBYFLOAT,
    INCRBY,
    INCRBYFLOAT,
    DECRBY,
    DEL,
    HDEL,
    NONE
}
