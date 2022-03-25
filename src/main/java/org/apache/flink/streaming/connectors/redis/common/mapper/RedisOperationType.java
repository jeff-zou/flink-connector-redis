package org.apache.flink.streaming.connectors.redis.common.mapper;

/**
 * @Author: jeff.zou @Date: 2022/3/23.14:10
 */
public enum RedisOperationType {
    /** query value from redis. */
    QUERY,
    /** del member at key. */
    DEL,
    /** add member at key. */
    INSERT,
    /** incr or decr at key. */
    ACC
}
