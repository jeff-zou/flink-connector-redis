package org.apache.flink.streaming.connectors.redis.common.mapper;

public enum RedisSinkCommand {
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
    HINCRBY,
    HINCRBYFLOAT,
    INCRBY,
    INCRBYFLOAT,
    DECRBY,
    DEL,
    HDEL,
    NONE
}
