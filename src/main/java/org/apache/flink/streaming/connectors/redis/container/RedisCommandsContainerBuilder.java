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

import org.apache.flink.streaming.connectors.redis.config.FlinkConfigBase;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisClient;
import io.lettuce.core.cluster.RedisClusterClient;

/** The builder for {@link RedisCommandsContainer}. */
public class RedisCommandsContainerBuilder {

    /**
     * @param flinkConfigBase
     * @return
     */
    public static RedisCommandsContainer build(FlinkConfigBase flinkConfigBase) {
        AbstractRedisClient redisClient = RedisClientBuilder.build(flinkConfigBase);
        if (redisClient instanceof RedisClusterClient) {
            return new RedisClusterContainer((RedisClusterClient) redisClient);
        } else {
            return new RedisContainer((RedisClient) redisClient);
        }
    }
}
