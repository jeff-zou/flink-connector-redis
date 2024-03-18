package org.apache.flink.streaming.connectors.redis.command;

public enum RedisSelectCommand {
    GET,
    HGET,
    LRANGE,
    SRANDMEMBER,
    ZSCORE,
    SUBSCRIBE,
    NONE
}
