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
import org.apache.flink.streaming.connectors.redis.config.RedisLookupConfig;
import org.apache.flink.streaming.connectors.redis.config.RedisOptions;
import org.apache.flink.streaming.connectors.redis.hanlder.RedisHandlerServices;
import org.apache.flink.streaming.connectors.redis.mapper.RedisMapper;
import org.apache.flink.streaming.connectors.redis.mapper.RowRedisQueryMapper;
import org.apache.flink.streaming.connectors.redis.stream.RedisLookupFunction;
import org.apache.flink.streaming.connectors.redis.stream.RedisSource;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.AsyncTableFunctionProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.util.Preconditions;

import java.util.Map;

/** redis dynamic table source. @Author: jeff.zou @Date: 2022/3/7.13:41 */
public class RedisDynamicTableSource implements ScanTableSource, LookupTableSource {

    private final FlinkConfigBase flinkConfigBase;
    private final RedisMapper redisMapper;
    private final RedisLookupConfig redisLookupConfig;
    private final RedisCommand redisCommand;
    private Map<String, String> properties;
    private ResolvedSchema resolvedSchema;
    private ReadableConfig config;

    public RedisDynamicTableSource(
            RedisCommand redisCommand,
            Map<String, String> properties,
            ResolvedSchema resolvedSchema,
            ReadableConfig config) {
        this.redisCommand = redisCommand;
        this.properties = properties;
        Preconditions.checkNotNull(properties, "properties should not be null");
        this.resolvedSchema = resolvedSchema;
        Preconditions.checkNotNull(resolvedSchema, "resolvedSchema should not be null");
        this.config = config;
        redisMapper = new RowRedisQueryMapper(redisCommand);
        this.properties = properties;
        this.resolvedSchema = resolvedSchema;
        this.config = config;
        flinkConfigBase =
                RedisHandlerServices.findRedisHandler(FlinkConfigHandler.class, properties)
                        .createFlinkConfig(config);
        redisLookupConfig =
                new RedisLookupConfig.Builder()
                        .setCacheTTL(config.get(RedisOptions.LOOKUP_CHCHE_TTL))
                        .setCacheMaxSize(config.get(RedisOptions.LOOKUP_CACHE_MAX_ROWS))
                        .setLoadAll(config.get(RedisOptions.LOOKUP_CACHE_LOAD_ALL))
                        .build();
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.all();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        RedisSource redisSource =
                new RedisSource<>(redisMapper, config, flinkConfigBase, resolvedSchema);
        return SourceProvider.of(redisSource);
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
        return AsyncTableFunctionProvider.of(
                new RedisLookupFunction(
                        flinkConfigBase, redisMapper, redisLookupConfig, resolvedSchema, config));
    }

    @Override
    public DynamicTableSource copy() {
        return new RedisDynamicTableSource(redisCommand, properties, resolvedSchema, config);
    }

    @Override
    public String asSummaryString() {
        return "REDIS";
    }
}
