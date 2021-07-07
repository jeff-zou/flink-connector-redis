package org.apache.flink.streaming.connectors.redis.common.mapper;

import org.apache.flink.api.common.functions.Function;

import java.io.Serializable;

public interface RedisMapper<T> extends Function, Serializable {

    /**
     * Returns descriptor which defines data type.
     *
     * @return data type descriptor
     */
    RedisCommandDescription getCommandDescription();

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
     *
     * @param data
     * @param fieldIndex
     * @return
     */
    String getFieldFromData(T data, Integer fieldIndex);

}
