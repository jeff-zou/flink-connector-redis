/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.redis.stream;

import static org.apache.flink.streaming.connectors.redis.table.RedisDynamicTableFactory.CACHE_SEPERATOR;

import org.apache.flink.streaming.connectors.redis.config.RedisValueDataStructure;
import org.apache.flink.streaming.connectors.redis.converter.RedisRowConverter;
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
            genericRowData.setField(
                    0,
                    RedisRowConverter.dataTypeFromString(
                            dataTypes.get(0).getLogicalType(), String.valueOf(keys[0])));
            if (value == null) {
                genericRowData.setField(0, null);
                return genericRowData;
            }

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
            genericRowData.setField(
                    0,
                    RedisRowConverter.dataTypeFromString(
                            dataTypes.get(0).getLogicalType(), String.valueOf(keys[0])));
            genericRowData.setField(
                    1,
                    RedisRowConverter.dataTypeFromString(
                            dataTypes.get(1).getLogicalType(), String.valueOf(keys[1])));

            if (value == null) {
                return genericRowData;
            }
            genericRowData.setField(
                    2,
                    RedisRowConverter.dataTypeFromString(dataTypes.get(2).getLogicalType(), value));
            return genericRowData;
        }
        return createRowDataForRow(value, dataTypes);
    }

    public static GenericRowData createRowDataForSortedSet(
            Object[] keys, Double value, List<DataType> dataTypes) {
        GenericRowData genericRowData = new GenericRowData(3);
        genericRowData.setField(
                0,
                RedisRowConverter.dataTypeFromString(
                        dataTypes.get(0).getLogicalType(), String.valueOf(keys[0])));
        genericRowData.setField(
                2,
                RedisRowConverter.dataTypeFromString(
                        dataTypes.get(2).getLogicalType(), String.valueOf(keys[1])));

        if (value == null) {
            return genericRowData;
        }
        genericRowData.setField(1, value);
        return genericRowData;
    }
}
