package org.apache.flink.streaming.connectors.redis.common.mapper;

/**
 *
 * @param <T>
 */
public interface RedisSinkMapper<T> extends RedisMapper<T> {

    /**
     * Extracts key from data.
     *
     * @param data source data
     * @return key
     */
    String getKeyFromData(T data, Integer keyIndex);

    /**
     * Extracts value from data.
     *
     * @param data source data
     * @return value
     */
    String getValueFromData(T data, Integer valueIndex);

    /**
     * @param data
     * @param fieldIndex
     * @return
     */
    String getFieldFromData(T data, Integer fieldIndex);
}
