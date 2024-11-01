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

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.connectors.redis.command.RedisCommand;
import org.apache.flink.streaming.connectors.redis.config.FlinkConfigBase;
import org.apache.flink.streaming.connectors.redis.config.FlinkConfigHandler;
import org.apache.flink.streaming.connectors.redis.config.RedisOptions;
import org.apache.flink.streaming.connectors.redis.hanlder.RedisHandlerServices;
import org.apache.flink.streaming.connectors.redis.mapper.RedisSinkMapper;
import org.apache.flink.streaming.connectors.redis.mapper.RowRedisSinkMapper;
import org.apache.flink.streaming.connectors.redis.stream.RedisSink;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;

import java.util.Map;

/** Created by jeff.zou on 2020/9/10. */
public class RedisDynamicTableSink implements DynamicTableSink {

    private final RedisCommand redisCommand;
    private final FlinkConfigBase flinkConfigBase;
    private final RedisSinkMapper redisMapper;
    private final Map<String, String> properties;
    private final ReadableConfig config;
    private final Integer sinkParallelism;
    private final ResolvedSchema resolvedSchema;

    public RedisDynamicTableSink(
            RedisCommand redisCommand,
            Map<String, String> properties,
            ResolvedSchema resolvedSchema,
            ReadableConfig config) {
        this.redisCommand = redisCommand;
        this.properties = properties;
        Preconditions.checkNotNull(properties, "properties should not be null");
        this.config = config;
        this.sinkParallelism = config.get(RedisOptions.SINK_PARALLELISM);
        redisMapper = new RowRedisSinkMapper(redisCommand, config);
        flinkConfigBase =
                RedisHandlerServices.findRedisHandler(FlinkConfigHandler.class, properties)
                        .createFlinkConfig(config);
        this.resolvedSchema = resolvedSchema;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.DELETE)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .build();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        RedisSink redisSink = new RedisSink(flinkConfigBase, redisMapper, resolvedSchema, config);
        return SinkV2Provider.of(redisSink, sinkParallelism);
    }

    @Override
    public DynamicTableSink copy() {
        return new RedisDynamicTableSink(redisCommand, properties, resolvedSchema, config);
    }

    @Override
    public String asSummaryString() {
        return "REDIS";
    }
}
