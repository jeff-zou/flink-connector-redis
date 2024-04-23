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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.connectors.redis.command.RedisCommand;
import org.apache.flink.streaming.connectors.redis.config.RedisOptions;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.util.FlinkRuntimeException;

import java.util.HashSet;
import java.util.Set;

/** Created by jeff.zou on 2020/9/10. */
public class RedisDynamicTableFactory
        implements DynamicTableSinkFactory, DynamicTableSourceFactory {

    public static final String IDENTIFIER = "redis";

    public static final String CACHE_SEPERATOR = "\01";

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        ReadableConfig config = helper.getOptions();
        helper.validate();
        RedisCommand redisCommand = parseCommand(config);

        return new RedisDynamicTableSource(
                redisCommand,
                context.getCatalogTable().getOptions(),
                context.getCatalogTable().getResolvedSchema(),
                config);
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        ReadableConfig config = helper.getOptions();
        helper.validate();
        RedisCommand redisCommand = parseCommand(config);
        return new RedisDynamicTableSink(
                redisCommand,
                context.getCatalogTable().getOptions(),
                context.getCatalogTable().getResolvedSchema(),
                config);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(RedisOptions.COMMAND);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(RedisOptions.DATABASE);
        options.add(RedisOptions.HOST);
        options.add(RedisOptions.PORT);
        options.add(RedisOptions.MAXIDLE);
        options.add(RedisOptions.MAXTOTAL);
        options.add(RedisOptions.CLUSTERNODES);
        options.add(RedisOptions.PASSWORD);
        options.add(RedisOptions.TIMEOUT);
        options.add(RedisOptions.MINIDLE);
        options.add(RedisOptions.REDISMODE);
        options.add(RedisOptions.TTL);
        options.add(RedisOptions.LOOKUP_CACHE_MAX_ROWS);
        options.add(RedisOptions.LOOKUP_CHCHE_TTL);
        options.add(RedisOptions.MAX_RETRIES);
        options.add(RedisOptions.SINK_PARALLELISM);
        options.add(RedisOptions.LOOKUP_CACHE_LOAD_ALL);
        options.add(RedisOptions.SINK_LIMIT);
        options.add(RedisOptions.SINK_LIMIT_MAX_NUM);
        options.add(RedisOptions.SINK_LIMIT_MAX_ONLINE);
        options.add(RedisOptions.SINK_LIMIT_INTERVAL);
        options.add(RedisOptions.VALUE_DATA_STRUCTURE);
        options.add(RedisOptions.REDIS_MASTER_NAME);
        options.add(RedisOptions.SENTINELS_INFO);
        options.add(RedisOptions.EXPIRE_ON_TIME);
        options.add(RedisOptions.SENTINELS_PASSWORD);
        options.add(RedisOptions.SET_IF_ABSENT);
        options.add(RedisOptions.TTL_KEY_NOT_ABSENT);
        options.add(RedisOptions.NETTY_EVENT_POOL_SIZE);
        options.add(RedisOptions.NETTY_IO_POOL_SIZE);
        options.add(RedisOptions.SCAN_KEY);
        options.add(RedisOptions.SCAN_ADDITION_KEY);
        options.add(RedisOptions.SCAN_RANGE_STOP);
        options.add(RedisOptions.SCAN_RANGE_START);
        options.add(RedisOptions.SCAN_COUNT);
        return options;
    }

    private RedisCommand parseCommand(ReadableConfig config) {
        try {
            return RedisCommand.valueOf(config.get(RedisOptions.COMMAND).toUpperCase());
        } catch (Exception e) {
            throw new FlinkRuntimeException(
                    String.format(
                            "do not support redis command: %s", config.get(RedisOptions.COMMAND)));
        }
    }
}
