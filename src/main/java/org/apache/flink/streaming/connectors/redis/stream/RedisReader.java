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

import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.SourceReaderBase;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.streaming.connectors.redis.command.RedisCommand;
import org.apache.flink.streaming.connectors.redis.config.FlinkConfigBase;
import org.apache.flink.streaming.connectors.redis.config.RedisOptions;
import org.apache.flink.streaming.connectors.redis.config.RedisValueDataStructure;
import org.apache.flink.streaming.connectors.redis.container.RedisCommandsContainer;
import org.apache.flink.streaming.connectors.redis.mapper.RedisMapper;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.types.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class RedisReader<T, SplitT extends SourceSplit> extends SourceReaderBase {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(RedisReader.class);
    private final int maxRetryTimes;
    private final RedisValueDataStructure redisValueDataStructure;
    private final List<DataType> dataTypes;
    private final FlinkConfigBase flinkConfigBase;
    ReadableConfig readableConfig;
    private final transient RedisCommandsContainer redisCommandsContainer;

    public RedisReader(
            FutureCompletingBlockingQueue<RecordsWithSplitIds> elementsQueue,
            SourceReaderContext sourceReaderContext,
            RedisCommandsContainer redisCommandsContainer,
            RedisMapper redisMapper,
            ReadableConfig readableConfig,
            FlinkConfigBase flinkConfigBase,
            ResolvedSchema resolvedSchema,
            Configuration config) {
        super(
                elementsQueue,
                null,
                new RedisRecordEmitter(redisMapper, redisCommandsContainer, readableConfig),
                config,
                sourceReaderContext);
        this.redisCommandsContainer = redisCommandsContainer;
        this.readableConfig = readableConfig;
        this.flinkConfigBase = flinkConfigBase;
        this.maxRetryTimes = readableConfig.get(RedisOptions.MAX_RETRIES);
        this.redisValueDataStructure = readableConfig.get(RedisOptions.VALUE_DATA_STRUCTURE);

        this.dataTypes = resolvedSchema.getColumnDataTypes();
    }

    @Override
    public void start() {
        try {
            this.redisCommandsContainer.open();
            LOG.info("success to create redis container.");
        } catch (Exception e) {
            LOG.error("Redis has not been properly initialized: ", e);
        }
    }

    @Override
    public void addSplits(List splits) {}

    @Override
    public void notifyNoMoreSplits() {}

    @Override
    public void close() throws Exception {
        super.close();
        if (redisCommandsContainer != null) {
            redisCommandsContainer.close();
        }
    }

    @Override
    protected Object initializedState(SourceSplit split) {
        return null;
    }

    @Override
    protected SourceSplit toSplitType(String splitId, Object splitState) {
        return null;
    }

    @Override
    protected void onSplitFinished(Map finishedSplitIds) {}
}
