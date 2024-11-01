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

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.connectors.redis.command.RedisCommand;
import org.apache.flink.streaming.connectors.redis.command.RedisCommandBaseDescription;
import org.apache.flink.streaming.connectors.redis.config.FlinkConfigBase;
import org.apache.flink.streaming.connectors.redis.config.RedisOptions;
import org.apache.flink.streaming.connectors.redis.config.RedisValueDataStructure;
import org.apache.flink.streaming.connectors.redis.container.RedisCommandsContainer;
import org.apache.flink.streaming.connectors.redis.container.RedisCommandsContainerBuilder;
import org.apache.flink.streaming.connectors.redis.mapper.RedisMapper;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RedisSource<T> implements Source {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(RedisSource.class);
    private final int maxRetryTimes;
    private final RedisValueDataStructure redisValueDataStructure;
    private final ResolvedSchema resolvedSchema;
    private final FlinkConfigBase flinkConfigBase;
    private final RedisCommand redisCommand;

    private final RedisMapper redisMapper;
    ReadableConfig readableConfig;

    public RedisSource(
            RedisMapper redisMapper,
            ReadableConfig readableConfig,
            FlinkConfigBase flinkConfigBase,
            ResolvedSchema resolvedSchema) {
        this.readableConfig = readableConfig;
        this.flinkConfigBase = flinkConfigBase;
        this.maxRetryTimes = readableConfig.get(RedisOptions.MAX_RETRIES);
        this.redisValueDataStructure = readableConfig.get(RedisOptions.VALUE_DATA_STRUCTURE);
        this.redisMapper = redisMapper;
        RedisCommandBaseDescription redisCommandDescription = redisMapper.getCommandDescription();
        Preconditions.checkNotNull(
                redisCommandDescription, "Redis Mapper data type description can not be null");
        this.redisCommand = redisCommandDescription.getRedisCommand();
        this.resolvedSchema = resolvedSchema;
    }

    @Override
    public Boundedness getBoundedness() {
        return this.redisCommand.isCommandBoundedness()
                ? Boundedness.BOUNDED
                : Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SourceReader createReader(SourceReaderContext readerContext) throws Exception {
        FutureCompletingBlockingQueue elementsQueue = new FutureCompletingBlockingQueue<>(8);
        RedisCommandsContainer redisCommandsContainer =
                RedisCommandsContainerBuilder.build(this.flinkConfigBase);
        return new RedisReader(
                elementsQueue,
                readerContext,
                redisCommandsContainer,
                this.redisMapper,
                this.readableConfig,
                this.flinkConfigBase,
                this.resolvedSchema,
                readerContext.getConfiguration());
    }

    @Override
    public SplitEnumerator createEnumerator(SplitEnumeratorContext enumContext) throws Exception {
        return null;
    }

    @Override
    public SplitEnumerator restoreEnumerator(SplitEnumeratorContext enumContext, Object checkpoint)
            throws Exception {
        return null;
    }

    @Override
    public SimpleVersionedSerializer getSplitSerializer() {
        return null;
    }

    @Override
    public SimpleVersionedSerializer getEnumeratorCheckpointSerializer() {
        return null;
    }
}
