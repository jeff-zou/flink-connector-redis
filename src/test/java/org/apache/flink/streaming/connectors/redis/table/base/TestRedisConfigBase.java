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

package org.apache.flink.streaming.connectors.redis.table.base;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;

/**
 * @Author: Jeff Zou @Date: 2022/10/14 10:07
 */
public class TestRedisConfigBase {

    private static final Logger LOG = LoggerFactory.getLogger(TestRedisConfigBase.class);

    public static final String REDIS_HOST = "10.11.69.176";
    public static final int REDIS_PORT = 6379;
    public static final String REDIS_PASSWORD = "test123";
    protected static StatefulRedisConnection<String, String> singleConnect;
    protected static RedisCommands singleRedisCommands;

    private static RedisClient redisClient;

    @BeforeAll
    public static void connectRedis() {
        RedisURI redisURI =
                RedisURI.builder()
                        .withHost(REDIS_HOST)
                        .withPort(REDIS_PORT)
                        .withPassword(REDIS_PASSWORD.toCharArray())
                        .build();
        redisClient = RedisClient.create(redisURI);
        singleConnect = redisClient.connect();
        singleRedisCommands = singleConnect.sync();
        LOG.info("connect to the redis: {}", REDIS_HOST);
    }

    @AfterAll
    public static void stopSingle() {
        singleConnect.close();
        redisClient.shutdown();
    }

    protected String singleWith() {
        return "'connector'='redis', "
                + "'host'='"
                + REDIS_HOST
                + "','port'='"
                + REDIS_PORT
                + "', 'redis-mode'='single','password'='"
                + REDIS_PASSWORD
                + "',";
    }
}
