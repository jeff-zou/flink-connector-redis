package org.apache.flink.streaming.connectors.redis.common.mapper;

public enum RedisQueryCommand {
    GET,
    HGET,
    HGETALL,
    LRANGE,
    SMEMBERS,
    ZRANGE,
    SUBSCRIBE,
    ZSCORE,
    PFCOUNT,
    NONE
}
