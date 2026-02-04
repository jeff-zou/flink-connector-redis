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

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.command.RedisCommand;
import org.apache.flink.streaming.connectors.redis.table.base.TestRedisConfigBase;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.jupiter.api.Test;
import org.junit.platform.commons.util.Preconditions;

import static org.apache.flink.streaming.connectors.redis.config.RedisValidator.REDIS_COMMAND;

/**
 * Tests for RedisDynamicTableSink metadata TTL functionality.
 */
public class SQLMetadataTTLTest extends TestRedisConfigBase {

    @Test
    public void testSetValidTTLSQL() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        String testKey = "test_metadata_ttl";
        String testKey2 = "test_metadata_ttl2";
        String ddl =
                "create table sink_redis_ttl(" +
                        "test_ttl_key VARCHAR, " +
                        "test_ttl_val VARCHAR," +
                        "ttl INT METADATA FROM 'ttl'" +
                        ") with ( 'connector'='redis', "
                        + "'host'='"
                        + REDIS_HOST
                        + "','port'='"
                        + REDIS_PORT
                        + "', 'redis-mode'='single','password'='"
                        + REDIS_PASSWORD
                        + "','"
                        + REDIS_COMMAND
                        + "'='"
                        + RedisCommand.SET
                        + "', 'audit.log'='true')";

        tEnv.executeSql(ddl);
        String sql =
                " insert into sink_redis_ttl select * from (values ('" + testKey + "', 'test_ttl_val', 3600), ('"
                        + testKey2 + "', 'test_ttl_val2', -1))";
        long start = System.currentTimeMillis();
        TableResult tableResult = tEnv.executeSql(sql);
        tableResult.getJobClient().get().getJobExecutionResult().get();
        long end = System.currentTimeMillis();
        Long ttl = singleRedisCommands.ttl(testKey);
        long expectTTL = 3600 - (end - start) / 1000;
        // allow 10 seconds of difference
        Preconditions.condition(
                Math.abs(ttl - expectTTL) < 10,
                "TTL not set correctly, expected around " + expectTTL + " but got " + ttl);
        Preconditions.condition(
                singleRedisCommands.ttl(testKey2) == -1, "TTL should be -1 for non-expiring keys");
        singleRedisCommands.del(testKey);
        singleRedisCommands.del(testKey2);
    }

    @Test
    public void testNoneTTLSQL() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        String testKey = "test_none_metadata_ttl";
        String ddl =
                "create table sink_redis_ttl(" +
                        "test_ttl_key VARCHAR, " +
                        "ttl INT" +
                        ") with ( 'connector'='redis', "
                        + "'host'='"
                        + REDIS_HOST
                        + "','port'='"
                        + REDIS_PORT
                        + "', 'redis-mode'='single','password'='"
                        + REDIS_PASSWORD
                        + "','"
                        + REDIS_COMMAND
                        + "'='"
                        + RedisCommand.SET
                        + "', 'audit.log'='true')";

        tEnv.executeSql(ddl);
        String sql =
                " insert into sink_redis_ttl select * from (values ('" + testKey + "', 3600))";
        TableResult tableResult = tEnv.executeSql(sql);
        tableResult.getJobClient().get().getJobExecutionResult().get();
        Long ttl = singleRedisCommands.ttl(testKey);
        Preconditions.condition(ttl == -1, "TTL should be -1 for non-expiring keys");
        singleRedisCommands.del(testKey);
    }

    @Test
    public void testSetInvalidTTLSQL() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        String testKey = "test_invalid_metadata_ttl";
        String ddl =
                "create table sink_redis_ttl(" +
                        "test_ttl_key VARCHAR, " +
                        "test_ttl_val VARCHAR," +
                        "ttl INT METADATA FROM 'ttl'" +
                        ") with ( 'connector'='redis', "
                        + "'host'='"
                        + REDIS_HOST
                        + "','port'='"
                        + REDIS_PORT
                        + "', 'redis-mode'='single','password'='"
                        + REDIS_PASSWORD
                        + "','"
                        + REDIS_COMMAND
                        + "'='"
                        + RedisCommand.SET
                        + "', 'audit.log'='true')";

        tEnv.executeSql(ddl);
        String sql =
                " insert into sink_redis_ttl select * from (values ('" + testKey + "', 'test_ttl_val', -100))";
        TableResult tableResult = tEnv.executeSql(sql);
        tableResult.getJobClient().get().getJobExecutionResult().get();
        Long ttl = singleRedisCommands.ttl(testKey);
        Preconditions.condition(
                ttl == -1,
                "TTL should be -1 for invalid metadata TTL configuration, but got " + ttl);
        singleRedisCommands.del(testKey);
    }
}
