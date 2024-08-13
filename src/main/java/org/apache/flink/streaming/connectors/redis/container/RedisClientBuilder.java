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

package org.apache.flink.streaming.connectors.redis.container;

import org.apache.flink.streaming.connectors.redis.config.FlinkClusterConfig;
import org.apache.flink.streaming.connectors.redis.config.FlinkConfigBase;
import org.apache.flink.streaming.connectors.redis.config.FlinkSentinelConfig;
import org.apache.flink.streaming.connectors.redis.config.FlinkSingleConfig;
import org.apache.flink.util.StringUtils;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.ClusterTopologyRefreshOptions;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.resource.DefaultClientResources;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @author Jeff Zou
 * @date 2024/3/20 17:08
 */
public class RedisClientBuilder {

    /**
     * Initialize the {@link RedisCommandsContainer} based on the instance type.
     *
     * @param flinkConfigBase configuration base
     * @return @throws IllegalArgumentException if Config, ClusterConfig and SentinelConfig are all
     *     null
     */
    public static AbstractRedisClient build(FlinkConfigBase flinkConfigBase) {
        DefaultClientResources.Builder builder = DefaultClientResources.builder();
        if (flinkConfigBase.getLettuceConfig() != null) {
            if (flinkConfigBase.getLettuceConfig().getNettyIoPoolSize() != null) {
                builder.ioThreadPoolSize(flinkConfigBase.getLettuceConfig().getNettyIoPoolSize());
            }
            if (flinkConfigBase.getLettuceConfig().getNettyEventPoolSize() != null) {
                builder.computationThreadPoolSize(
                        flinkConfigBase.getLettuceConfig().getNettyEventPoolSize());
            }
        }

        ClientResources clientResources = builder.build();

        if (flinkConfigBase instanceof FlinkSingleConfig) {
            return build((FlinkSingleConfig) flinkConfigBase, clientResources);
        } else if (flinkConfigBase instanceof FlinkClusterConfig) {
            return build((FlinkClusterConfig) flinkConfigBase, clientResources);
        } else if (flinkConfigBase instanceof FlinkSentinelConfig) {
            return build((FlinkSentinelConfig) flinkConfigBase, clientResources);
        } else {
            throw new IllegalArgumentException("configuration not found!");
        }
    }

    /**
     * Builds container for single Redis environment.
     *
     * @param singleConfig configuration for redis
     * @return container for single Redis environment
     * @throws NullPointerException if singleConfig is null
     */
    private static RedisClient build(
            FlinkSingleConfig singleConfig, ClientResources clientResources) {
        Objects.requireNonNull(singleConfig, "Redis config should not be Null");

        RedisURI.Builder builder =
                RedisURI.builder()
                        .withHost(singleConfig.getHost())
                        .withPort(singleConfig.getPort())
                        .withDatabase(singleConfig.getDatabase())
                        .withSsl(singleConfig.getSsl());
        if (!StringUtils.isNullOrWhitespaceOnly(singleConfig.getPassword())) {
            builder.withPassword(singleConfig.getPassword().toCharArray());
        }

        return RedisClient.create(clientResources, builder.build());
    }

    /**
     * Builds container for Redis Cluster environment.
     *
     * @param clusterConfig configuration for Cluster
     * @return container for Redis Cluster environment
     * @throws NullPointerException if ClusterConfig is null
     */
    private static RedisClusterClient build(
            FlinkClusterConfig clusterConfig, ClientResources clientResources) {
        Objects.requireNonNull(clusterConfig, "Redis cluster config should not be Null");

        List<RedisURI> redisURIS =
                Arrays.stream(clusterConfig.getNodesInfo().split(","))
                        .map(
                                node -> {
                                    String[] redis = node.split(":");
                                    RedisURI.Builder builder =
                                            RedisURI.builder()
                                                    .withHost(redis[0])
                                                    .withPort(Integer.parseInt(redis[1]))
                                                    .withSsl(clusterConfig.getSsl());;
                                    if (!StringUtils.isNullOrWhitespaceOnly(
                                            clusterConfig.getPassword())) {
                                        builder.withPassword(
                                                clusterConfig.getPassword().toCharArray());
                                    }
                                    return builder.build();
                                })
                        .collect(Collectors.toList());

        RedisClusterClient clusterClient = RedisClusterClient.create(clientResources, redisURIS);

        ClusterTopologyRefreshOptions topologyRefreshOptions =
                ClusterTopologyRefreshOptions.builder()
                        .enableAdaptiveRefreshTrigger(
                                ClusterTopologyRefreshOptions.RefreshTrigger.MOVED_REDIRECT,
                                ClusterTopologyRefreshOptions.RefreshTrigger.PERSISTENT_RECONNECTS)
                        .adaptiveRefreshTriggersTimeout(Duration.ofSeconds(10L))
                        .build();

        clusterClient.setOptions(
                ClusterClientOptions.builder()
                        .topologyRefreshOptions(topologyRefreshOptions)
                        .build());

        return clusterClient;
    }

    /**
     * @param sentinelConfig
     * @param clientResources
     * @return
     */
    private static RedisClient build(
            FlinkSentinelConfig sentinelConfig, ClientResources clientResources) {
        Objects.requireNonNull(sentinelConfig, "Redis sentinel config should not be Null");

        RedisURI.Builder builder =
                RedisURI.builder()
                        .withSentinelMasterId(sentinelConfig.getMasterName())
                        .withDatabase(sentinelConfig.getDatabase());

        Arrays.stream(sentinelConfig.getSentinelsInfo().split(","))
                .forEach(
                        node -> {
                            String[] redis = node.split(":");
                            if (StringUtils.isNullOrWhitespaceOnly(sentinelConfig.getPassword())) {
                                builder.withSentinel(
                                        redis[0],
                                        Integer.parseInt(redis[1]),
                                        sentinelConfig.getSentinelsPassword());
                            } else {
                                builder.withSentinel(
                                        redis[0],
                                        Integer.parseInt(redis[1]),
                                        sentinelConfig.getSentinelsPassword())
                                        .withPassword(sentinelConfig.getPassword());
                            }
                        });

        return RedisClient.create(clientResources, builder.build());
    }
}
