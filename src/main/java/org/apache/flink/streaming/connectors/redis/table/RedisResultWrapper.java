package org.apache.flink.streaming.connectors.redis.table;

import static org.apache.flink.streaming.connectors.redis.table.RedisDynamicTableFactory.CACHE_SEPERATOR;

import org.apache.flink.streaming.connectors.redis.common.config.RedisValueDataStructure;
import org.apache.flink.streaming.connectors.redis.common.converter.RedisRowConverter;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.types.DataType;

import java.util.List;

public class RedisResultWrapper {

    /**
     * create row data for string.
     *
     * @param keys
     * @param value
     */
    public static GenericRowData createRowDataForString(
            Object[] keys,
            String value,
            RedisValueDataStructure redisValueDataStructure,
            List<DataType> dataTypes) {
        if (redisValueDataStructure == RedisValueDataStructure.column) {
            GenericRowData genericRowData = new GenericRowData(2);
            if (value == null) {
                return genericRowData;
            }
            genericRowData.setField(
                    0,
                    RedisRowConverter.dataTypeFromString(
                            dataTypes.get(0).getLogicalType(), String.valueOf(keys[0])));
            genericRowData.setField(
                    1,
                    RedisRowConverter.dataTypeFromString(dataTypes.get(1).getLogicalType(), value));
            return genericRowData;
        }

        return createRowDataForRow(value, dataTypes);
    }

    /**
     * create row data for whole row.
     *
     * @param value
     * @return
     */
    public static GenericRowData createRowDataForRow(String value, List<DataType> dataTypes) {
        GenericRowData genericRowData = new GenericRowData(dataTypes.size());
        if (value == null) {
            return genericRowData;
        }

        String[] values = value.split(CACHE_SEPERATOR);
        for (int i = 0; i < dataTypes.size(); i++) {
            if (i < values.length) {
                genericRowData.setField(
                        i,
                        RedisRowConverter.dataTypeFromString(
                                dataTypes.get(i).getLogicalType(), values[i]));
            } else {
                genericRowData.setField(i, null);
            }
        }
        return genericRowData;
    }

    /**
     * create row data for hash.
     *
     * @param keys
     * @param value
     */
    public static GenericRowData createRowDataForHash(
            Object[] keys,
            String value,
            RedisValueDataStructure redisValueDataStructure,
            List<DataType> dataTypes) {
        if (redisValueDataStructure == RedisValueDataStructure.column) {
            GenericRowData genericRowData = new GenericRowData(3);
            if (value == null) {
                return genericRowData;
            }
            genericRowData.setField(
                    0,
                    RedisRowConverter.dataTypeFromString(
                            dataTypes.get(0).getLogicalType(), String.valueOf(keys[0])));
            genericRowData.setField(
                    1,
                    RedisRowConverter.dataTypeFromString(
                            dataTypes.get(0).getLogicalType(), String.valueOf(keys[1])));
            genericRowData.setField(
                    2,
                    RedisRowConverter.dataTypeFromString(dataTypes.get(2).getLogicalType(), value));
            return genericRowData;
        }
        return createRowDataForRow(value, dataTypes);
    }
}
