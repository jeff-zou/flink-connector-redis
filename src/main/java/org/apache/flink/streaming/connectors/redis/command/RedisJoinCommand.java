package org.apache.flink.streaming.connectors.redis.command;

public enum RedisJoinCommand {
    GET,
    HGET,
    ZSCORE,
    NONE
}
