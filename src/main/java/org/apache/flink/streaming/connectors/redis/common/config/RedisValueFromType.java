package org.apache.flink.streaming.connectors.redis.common.config;

/** where does the value come from. @Author: Jeff Zou @Date: 2022/9/28 15:53 */
public enum RedisValueFromType {
    // The value will come from a field (for example, set: key is the first field defined by DDL,
    // and value is the second field).
    column,
    // value is taken from the entire row, separated by '\01'.
    row
}
