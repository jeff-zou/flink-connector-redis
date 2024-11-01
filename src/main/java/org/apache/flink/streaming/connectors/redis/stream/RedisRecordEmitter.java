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

import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.streaming.connectors.redis.command.RedisCommand;
import org.apache.flink.streaming.connectors.redis.command.RedisCommandBaseDescription;
import org.apache.flink.streaming.connectors.redis.command.RedisSelectCommand;
import org.apache.flink.streaming.connectors.redis.config.RedisOptions;
import org.apache.flink.streaming.connectors.redis.container.RedisCommandsContainer;
import org.apache.flink.streaming.connectors.redis.mapper.RedisMapper;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @author Jeff Zou
 * @date 2024/10/24 17:53
 */
public class RedisRecordEmitter implements RecordEmitter {

    private static final Logger LOG = LoggerFactory.getLogger(RedisSource.class);

    private final RedisCommand redisCommand;

    private final RedisCommandsContainer redisCommandsContainer;

    private final String[] queryParameter;

    private final ReadableConfig readableConfig;

    public RedisRecordEmitter(
            RedisMapper redisMapper,
            RedisCommandsContainer redisCommandsContainer,
            ReadableConfig readableConfig) {
        validator();
        RedisCommandBaseDescription redisCommandDescription = redisMapper.getCommandDescription();
        Preconditions.checkNotNull(
                redisCommandDescription, "Redis Mapper data type description can not be null");
        this.redisCommand = redisCommandDescription.getRedisCommand();
        this.redisCommandsContainer = redisCommandsContainer;
        this.readableConfig = readableConfig;

        this.queryParameter = new String[2];
        this.queryParameter[0] = this.readableConfig.get(RedisOptions.SCAN_KEY);
        if (this.redisCommand.getSelectCommand() == RedisSelectCommand.HGET) {
            this.queryParameter[1] = this.readableConfig.get(RedisOptions.SCAN_ADDITION_KEY);
        } else if (redisCommand.getSelectCommand() == RedisSelectCommand.ZSCORE) {
            this.queryParameter[1] = this.readableConfig.get(RedisOptions.SCAN_ADDITION_KEY);
        }
    }

    @Override
    public void emitRecord(Object element, SourceOutput output, Object splitState)
            throws Exception {
        output.collect(element);
    }

    @Override
    public void run(SourceContext ctx) throws Exception {
        // It will try many times which less than {@code maxRetryTimes} until execute success.
        for (int i = 0; i <= maxRetryTimes; i++) {
            try {
                query(ctx);
                break;
            } catch (Exception e) {
                LOG.error("query redis error, retry times:{}", i, e);
                if (i >= maxRetryTimes) {
                    throw new RuntimeException("query redis error ", e);
                }
                Thread.sleep(500L * i);
            }
        }
    }

    private void query(SourceContext ctx) throws Exception {
        switch (redisCommand.getSelectCommand()) {
            case GET:
                {
                    String result = this.redisCommandsContainer.get(queryParameter[0]).get();
                    GenericRowData rowData =
                            RedisResultWrapper.createRowDataForString(
                                    queryParameter, result, redisValueDataStructure, dataTypes);
                    ctx.collect(rowData);
                    break;
                }
            case HGET:
                {
                    String result =
                            this.redisCommandsContainer
                                    .hget(queryParameter[0], queryParameter[1])
                                    .get();
                    GenericRowData rowData =
                            RedisResultWrapper.createRowDataForHash(
                                    queryParameter, result, redisValueDataStructure, dataTypes);
                    ctx.collect(rowData);
                    break;
                }
            case ZSCORE:
                {
                    Double result =
                            this.redisCommandsContainer
                                    .zscore(queryParameter[0], queryParameter[1])
                                    .get();
                    GenericRowData rowData =
                            RedisResultWrapper.createRowDataForSortedSet(
                                    queryParameter, result, dataTypes);
                    ctx.collect(rowData);
                    break;
                }
            case LRANGE:
                {
                    List list =
                            this.redisCommandsContainer
                                    .lRange(
                                            queryParameter[0],
                                            this.readableConfig.get(RedisOptions.SCAN_RANGE_START),
                                            this.readableConfig.get(RedisOptions.SCAN_RANGE_STOP))
                                    .get();
                    list.forEach(
                            result -> {
                                GenericRowData rowData =
                                        RedisResultWrapper.createRowDataForString(
                                                queryParameter,
                                                String.valueOf(result),
                                                redisValueDataStructure,
                                                dataTypes);
                                ctx.collect(rowData);
                            });

                    break;
                }
            case SRANDMEMBER:
                {
                    List list =
                            this.redisCommandsContainer
                                    .srandmember(
                                            String.valueOf(queryParameter[0]),
                                            readableConfig.get(RedisOptions.SCAN_COUNT))
                                    .get();

                    list.forEach(
                            result -> {
                                GenericRowData rowData =
                                        RedisResultWrapper.createRowDataForString(
                                                queryParameter,
                                                String.valueOf(result),
                                                redisValueDataStructure,
                                                dataTypes);
                                ctx.collect(rowData);
                            });
                    break;
                }
            case SUBSCRIBE:
                {
                }
            default:
        }
    }

    private void validator() {
        Preconditions.checkNotNull(
                this.readableConfig.get(RedisOptions.SCAN_KEY),
                "the %s for source can not be null",
                RedisOptions.SCAN_KEY.key());

        Preconditions.checkArgument(
                redisCommand.getSelectCommand() != RedisSelectCommand.NONE,
                String.format("the command %s do not support query.", redisCommand.name()));

        if (redisCommand.getSelectCommand() == RedisSelectCommand.HGET) {
            Preconditions.checkNotNull(
                    this.readableConfig.get(RedisOptions.SCAN_ADDITION_KEY),
                    "must set field value of Map to %s",
                    RedisOptions.SCAN_ADDITION_KEY.key());
        } else if (redisCommand.getSelectCommand() == RedisSelectCommand.ZSCORE) {
            Preconditions.checkNotNull(
                    this.readableConfig.get(RedisOptions.SCAN_ADDITION_KEY),
                    "must set member value of SortedSet to %s",
                    RedisOptions.SCAN_ADDITION_KEY.key());
            Preconditions.checkArgument(
                    dataTypes.get(1).getLogicalType() instanceof DoubleType,
                    "the second column's type of source table must be double. the type of score is double when the data structure in redis is SortedSet.");
        } else if (redisCommand.getSelectCommand() == RedisSelectCommand.LRANGE) {
            Preconditions.checkNotNull(
                    this.readableConfig.get(RedisOptions.SCAN_RANGE_START),
                    "the %s must not be null when query list",
                    RedisOptions.SCAN_RANGE_START.key());

            Preconditions.checkNotNull(
                    this.readableConfig.get(RedisOptions.SCAN_RANGE_STOP),
                    "the %s must not be null when query list",
                    RedisOptions.SCAN_RANGE_STOP.key());
        } else if (redisCommand.getSelectCommand() == RedisSelectCommand.SRANDMEMBER) {
            Preconditions.checkNotNull(
                    this.readableConfig.get(RedisOptions.SCAN_COUNT),
                    "the %s must not be null when query set",
                    RedisOptions.SCAN_COUNT.key());
        }
    }
}
