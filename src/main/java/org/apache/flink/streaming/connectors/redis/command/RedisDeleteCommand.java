package org.apache.flink.streaming.connectors.redis.command;

public enum RedisDeleteCommand {
    SREM,
    DEL,
    ZREM,
    ZINCRBY,
    HDEL,
    HINCRBY,
    HINCRBYFLOAT,
    INCRBY,
    INCRBYFLOAT,
    NONE
}
