package org.apache.flink.streaming.connectors.redis.common.mapper;

public enum RedisDelCommand {
    SREM,
    DEL,
    ZREM,
    ZINCRBY,
    HDEL,
    HINCRBY,
    HINCRBYFLOAT,
    INCRBY,
    INCRBYFLOAT,
    DECRBY,
    LREM,
    NONE
}
