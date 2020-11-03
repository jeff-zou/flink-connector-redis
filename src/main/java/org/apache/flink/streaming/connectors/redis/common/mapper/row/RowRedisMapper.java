/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.redis.common.mapper.row;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.redis.common.hanlder.RedisMapperHandler;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.streaming.connectors.redis.descriptor.RedisValidator.REDIS_COMMAND;

/**
 * base row redis mapper implement.
 */
public abstract class RowRedisMapper implements RedisMapper<GenericRowData>, RedisMapperHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(RowRedisMapper.class);

    public static final char REDIS_VALUE_SEPERATOR = '\01';

    private Integer ttl;

    private RedisCommand redisCommand;

    private String additionalKey;

    private String partitionColumn;

    public String getAdditionalKey() {
        return additionalKey;
    }

    public void setAdditionalKey(String additionalKey) {
        this.additionalKey = additionalKey;
    }

    public Integer getTtl() {
        return ttl;
    }

    public void setTtl(int ttl) {
        this.ttl = ttl;
    }

    public RedisCommand getRedisCommand() {
        return redisCommand;
    }

    public void setRedisCommand(RedisCommand redisCommand) {
        this.redisCommand = redisCommand;
    }

    public String getPartitionColumn() {
        return partitionColumn;
    }

    public void setPartitionColumn(String partitionColumn) {
        this.partitionColumn = partitionColumn;
    }

    public RowRedisMapper() {
    }

    public RowRedisMapper(int ttl, RedisCommand redisCommand, String partitionColumn) {
        this.ttl = ttl;
        this.redisCommand = redisCommand;
        this.partitionColumn = partitionColumn;
    }

    public RowRedisMapper( String additionalKey, RedisCommand redisCommand, String partitionColumn) {
        this.additionalKey = additionalKey;
        this.redisCommand = redisCommand;
        this.partitionColumn = partitionColumn;
    }

    public RowRedisMapper(RedisCommand redisCommand) {
        this.redisCommand = redisCommand;
    }

    public RowRedisMapper(int ttl, String additionalKey, RedisCommand redisCommand, String partitionColumn) {
        this.ttl = ttl;
        this.additionalKey = additionalKey;
        this.redisCommand = redisCommand;
        this.partitionColumn = partitionColumn;
    }

    @Override
    public RedisCommandDescription getCommandDescription() {
        return new RedisCommandDescription(redisCommand, additionalKey, ttl, partitionColumn);
    }

    @Override
    public String getKeyFromData(GenericRowData row, List<Integer> keyIndexs) {
        StringBuilder sb = new StringBuilder();
        for(int i=0;i<keyIndexs.size();i++){
            if(i!=0)
                sb.append(REDIS_VALUE_SEPERATOR);
            Integer index = keyIndexs.get(i);
            sb.append(row.getField(index));

        }
        return sb.toString();
    }

    @Override
    public String getValueFromData(GenericRowData row, List<Integer> valueIndexs) {
        StringBuilder sb = new StringBuilder();
        for(int i=0;i<valueIndexs.size();i++){
            if(i!=0)
                sb.append(REDIS_VALUE_SEPERATOR);
            Integer index = valueIndexs.get(i);
            sb.append(row.getField(index));

        }
        return sb.toString();
    }


    @Override
    public String getPartitionFromData(GenericRowData row, Integer partitionColumnIndexs) {
        return String.valueOf(row.getField(partitionColumnIndexs));
    }

    @Override
    public Map<String, String> requiredContext() {
        Map<String, String> require = new HashMap<>();
        require.put(REDIS_COMMAND, getRedisCommand().name());
        return require;
    }

    @Override
    public boolean equals(Object obj) {
        RedisCommand redisCommand = ((RowRedisMapper) obj).redisCommand;
        return this.redisCommand == redisCommand;
    }

    @Override
    public Optional<Integer> getAdditionalTTL(GenericRowData row) {
        return Optional.ofNullable(getTtl());
    }
}
