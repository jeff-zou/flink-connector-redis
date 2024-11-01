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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @param <IN>
 */
public class RedisSink<IN> implements Sink<IN> {

    private static final Logger LOG = LoggerFactory.getLogger(RedisSink.class);
    private static final long serialVersionUID = 1L;
    private final FlinkConfigBase flinkConfigBase;
    private final RedisSinkMapper<IN> redisSinkMapper;
    private final ResolvedSchema resolvedSchema;
    private final ReadableConfig readableConfig;

    /**
     * Creates a new {@link RedisSink} that connects to the Redis server.
     *
     * @param flinkConfigBase The configuration of {@link FlinkConfigBase}
     * @param redisSinkMapper This is used to generate Redis command and key value from incoming
     *     elements.
     */
    public RedisSink(
            FlinkConfigBase flinkConfigBase,
            RedisSinkMapper<IN> redisSinkMapper,
            ResolvedSchema resolvedSchema,
            ReadableConfig readableConfig) {
        this.flinkConfigBase = flinkConfigBase;
        this.readableConfig = readableConfig;
        this.resolvedSchema = resolvedSchema;
        this.redisSinkMapper = redisSinkMapper;
    }

    @Override
    public RedisWriter<IN> createWriter(InitContext context) throws IOException {
        try {
            if (readableConfig.get(RedisOptions.SINK_LIMIT)) {
                return new LimitedRedisWriter<IN>(
                        flinkConfigBase, redisSinkMapper, resolvedSchema, readableConfig, context);
            } else {
                return new RedisWriter<IN>(
                        flinkConfigBase, redisSinkMapper, resolvedSchema, readableConfig, context);
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
