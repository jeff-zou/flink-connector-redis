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

package org.apache.flink.streaming.connectors.redis.config;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.util.Preconditions;

import java.util.HashMap;
import java.util.Map;

/** */
public class FlinkSingleConfigHandler implements FlinkConfigHandler {

    @Override
    public FlinkConfigBase createFlinkConfig(ReadableConfig config) {
        String host = config.get(RedisOptions.HOST);
        Preconditions.checkNotNull(host, "host should not be null in single mode");

        LettuceConfig lettuceConfig =
                new LettuceConfig(
                        config.get(RedisOptions.NETTY_IO_POOL_SIZE),
                        config.get(RedisOptions.NETTY_EVENT_POOL_SIZE));

        FlinkSingleConfig.Builder builder =
                new FlinkSingleConfig.Builder()
                        .setHost(host)
                        .setPassword(config.get(RedisOptions.PASSWORD))
                        .setSsl(config.get(RedisOptions.SSL))
                        .setLettuceConfig(lettuceConfig);
        builder.setPort(config.get(RedisOptions.PORT));
        builder.setTimeout(config.get(RedisOptions.TIMEOUT))
                .setDatabase(config.get(RedisOptions.DATABASE));

        return builder.build();
    }

    @Override
    public Map<String, String> requiredContext() {
        Map<String, String> require = new HashMap<>();
        require.put(RedisValidator.REDIS_MODE, RedisValidator.REDIS_SINGLE);
        return require;
    }

    public FlinkSingleConfigHandler() {
    }
}
