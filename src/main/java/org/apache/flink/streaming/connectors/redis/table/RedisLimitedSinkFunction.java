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

package org.apache.flink.streaming.connectors.redis.table;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.connectors.redis.config.FlinkConfigBase;
import org.apache.flink.streaming.connectors.redis.config.RedisOptions;
import org.apache.flink.streaming.connectors.redis.mapper.RedisSinkMapper;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * @Author: Jeff Zou @Date: 2022/9/26 15:28 Specially used for Flink online debugging.
 */
public class RedisLimitedSinkFunction<IN> extends RedisSinkFunction<IN> {

    private static final Logger LOG = LoggerFactory.getLogger(RedisLimitedSinkFunction.class);

    private long maxOnline;

    private long startTime;

    private long sinkInterval;

    private int maxNum;

    private volatile int curNum;

    /**
     * Creates a new {@link RedisSinkFunction} that connects to the Redis server.
     *
     * @param flinkConfigBase The configuration of {@link FlinkConfigBase}
     * @param redisSinkMapper This is used to generate Redis command and key value from incoming
     * @param resolvedSchema
     */
    public RedisLimitedSinkFunction(
            FlinkConfigBase flinkConfigBase,
            RedisSinkMapper<IN> redisSinkMapper,
            ResolvedSchema resolvedSchema,
            ReadableConfig config) {
        this(flinkConfigBase, redisSinkMapper, resolvedSchema, config, -1);
    }

    public RedisLimitedSinkFunction(
            FlinkConfigBase flinkConfigBase,
            RedisSinkMapper<IN> redisSinkMapper,
            ResolvedSchema resolvedSchema,
            ReadableConfig config,
            int ttlMetadataPosition) {
        super(flinkConfigBase, redisSinkMapper, resolvedSchema, config, ttlMetadataPosition);
        maxOnline = config.get(RedisOptions.SINK_LIMIT_MAX_ONLINE);

        Preconditions.checkState(
                maxOnline > 0 && maxOnline <= RedisOptions.SINK_LIMIT_MAX_ONLINE.defaultValue(),
                "the max online milliseconds must be more than 0 and less than %s seconds.",
                RedisOptions.SINK_LIMIT_MAX_ONLINE.defaultValue());

        sinkInterval = config.get(RedisOptions.SINK_LIMIT_INTERVAL);
        Preconditions.checkState(
                sinkInterval >= RedisOptions.SINK_LIMIT_INTERVAL.defaultValue(),
                "the sink limit interval must be more than % millisecond",
                RedisOptions.SINK_LIMIT_INTERVAL.defaultValue());

        maxNum = config.get(RedisOptions.SINK_LIMIT_MAX_NUM);
        Preconditions.checkState(
                maxNum > 0 && maxNum <= RedisOptions.SINK_LIMIT_MAX_NUM.defaultValue(),
                "the max num must be more than 0 and less than %s.",
                RedisOptions.SINK_LIMIT_MAX_NUM.defaultValue());
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        startTime = System.currentTimeMillis();
    }

    @Override
    public void invoke(IN input, Context context) throws Exception {
        long remainTime = maxOnline - (System.currentTimeMillis() - startTime);
        if (remainTime < 0) {
            throw new RuntimeException(
                    "thread id:"
                            + Thread.currentThread().getId()
                            + ", the debugging time has exceeded the max online time.");
        }

        RowData rowData = (RowData) input;
        RowKind kind = rowData.getRowKind();
        if (kind == RowKind.UPDATE_BEFORE) {
            return;
        }

        // all keys must expire 10 seconds after online debugging end.
        // if there's TTL from metadata, we should respect that instead
        if (this.getTtlMetadataPosition() < 0) {
            super.defaultTtl = (int) remainTime / 1000 + 10;
        }
        super.invoke(input, context);

        TimeUnit.MILLISECONDS.sleep(sinkInterval);
        curNum++;
        if (curNum > maxNum) {
            throw new RuntimeException(
                    "thread id:"
                            + Thread.currentThread().getId()
                            + ", the number of debug results has exceeded the max num."
                            + curNum);
        }
    }
}
