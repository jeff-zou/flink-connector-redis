package org.apache.flink.streaming.connectors.redis.common.config;

/** redis value data structure. @Author: Jeff Zou @Date: 2022/9/28 15:53 */
public enum RedisValueDataStructure {
    // The value will come from a field (for example, set: key is the first field defined by DDL,
    // and value is the second field).
    column,
    // value is taken from the entire row, separated by '\01'.
    row
}
