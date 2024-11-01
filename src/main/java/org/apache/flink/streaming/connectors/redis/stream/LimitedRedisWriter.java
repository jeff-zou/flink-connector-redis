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

import org.apache.flink.api.connector.sink2.Sink;
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

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * @author Jeff Zou
 * @date 2024/10/23 14:01
 */
public class LimitedRedisWriter<IN> extends RedisWriter<IN> {
    private static final Logger LOG = LoggerFactory.getLogger(LimitedRedisWriter.class);
    private final long maxOnline;
    private final long sinkInterval;
    private final int maxNum;
    protected Integer ttl;
    protected int expireTimeSeconds = -1;
    private final long startTime;
    private volatile int curNum;

    public LimitedRedisWriter(
            FlinkConfigBase flinkConfigBase,
            RedisSinkMapper<IN> redisSinkMapper,
            ResolvedSchema resolvedSchema,
            ReadableConfig config,
            Sink.InitContext sinkInitContext)
            throws Exception {
        super(flinkConfigBase, redisSinkMapper, resolvedSchema, config, sinkInitContext);
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
        startTime = System.currentTimeMillis();
    }

    @Override
    public void write(Object element, Context context) throws IOException, InterruptedException {
        long remainTime = maxOnline - (System.currentTimeMillis() - startTime);
        if (remainTime < 0) {
            throw new RuntimeException(
                    "thread id:"
                            + Thread.currentThread().getId()
                            + ", the debugging time has exceeded the max online time.");
        }

        RowData rowData = (RowData) element;
        RowKind kind = rowData.getRowKind();
        if (kind == RowKind.UPDATE_BEFORE) {
            return;
        }

        // all keys must expire 10 seconds after online debugging end.
        super.ttl = (int) remainTime / 1000 + 10;
        super.write(element, context);

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

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {}
}
