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

import static org.apache.flink.streaming.connectors.redis.config.RedisValidator.REDIS_CLUSTER;
import static org.apache.flink.streaming.connectors.redis.config.RedisValidator.REDIS_MODE;

/** cluster config handler to find and create cluster config use meta. */
public class FlinkClusterConfigHandler implements FlinkConfigHandler {

    @Override
    public FlinkConfigBase createFlinkConfig(ReadableConfig config) {
        Preconditions.checkState(
                config.get(RedisOptions.DATABASE) == 0, "redis cluster just support db 0");
        String nodesInfo = config.get(RedisOptions.CLUSTERNODES);
        Preconditions.checkNotNull(nodesInfo, "nodes should not be null in cluster mode");

        LettuceConfig lettuceConfig =
                new LettuceConfig(
                        config.get(RedisOptions.NETTY_IO_POOL_SIZE),
                        config.get(RedisOptions.NETTY_EVENT_POOL_SIZE));

        FlinkClusterConfig.Builder builder =
                new FlinkClusterConfig.Builder()
                        .setNodesInfo(nodesInfo)
                        .setPassword(config.get(RedisOptions.PASSWORD))
                        .setSsl(config.get(RedisOptions.SSL))
                        .setLettuceConfig(lettuceConfig);

        builder.setTimeout(config.get(RedisOptions.TIMEOUT));

        return builder.build();
    }

    @Override
    public Map<String, String> requiredContext() {
        Map<String, String> require = new HashMap<>();
        require.put(REDIS_MODE, REDIS_CLUSTER);
        return require;
    }

    public FlinkClusterConfigHandler() {
    }
}
