package org.apache.flink.streaming.connectors.redis.common.mapper;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;

/** @param <T> */
public interface RedisSinkMapper<T> extends RedisMapper<T> {

    /**
     * Extracts key from data.
     *
     * @param rowData source data
     * @return key
     */
    String getKeyFromData(RowData rowData, LogicalType logicalType, Integer keyIndex);

    /**
     * Extracts value from data.
     *
     * @param rowData source data
     * @return value
     */
    String getValueFromData(RowData rowData, LogicalType logicalType, Integer valueIndex);

    /**
     * @param rowData
     * @param fieldIndex
     * @return
     */
    String getFieldFromData(RowData rowData, LogicalType logicalType, Integer fieldIndex);
}
