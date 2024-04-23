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

import java.time.LocalTime;

import static org.apache.flink.streaming.connectors.redis.config.RedisValidator.REDIS_COMMAND;

/** Created by jeff.zou on 2020/9/10. */
public class SQLExpireTest extends TestRedisConfigBase {

    @Test
    public void testSinkValueWithExpire() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        singleRedisCommands.del("1");
        String ddl =
                "create table source_table(uid VARCHAR) with ('connector'='datagen',"
                        + "'rows-per-second'='1', "
                        + "'fields.uid.kind'='sequence', 'fields.uid.start'='1', 'fields.uid.end'='1')";
        tEnv.executeSql(ddl);

        String sink =
                "create table sink_redis(name varchar, level varchar, age varchar) with (  "
                        + sigleWith()
                        + "'ttl'='10', '"
                        + REDIS_COMMAND
                        + "'='"
                        + RedisCommand.HSET
                        + "' )";
        tEnv.executeSql(sink);
        String sql = " insert into sink_redis select '1', '1', uid from source_table";

        TableResult tableResult = tEnv.executeSql(sql);
        tableResult.getJobClient().get().getJobExecutionResult().get();
        System.out.println(sql);
        Preconditions.condition(singleRedisCommands.exists("1") == 1, "");
        Thread.sleep(10 * 1000);
        Preconditions.condition(singleRedisCommands.exists("1") == 0, "");
    }

    @Test
    public void testSinkValueWithExpireOnTime() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        singleRedisCommands.del("1");
        LocalTime localTime = LocalTime.now();
        int wait = 8;
        localTime = localTime.plusSeconds(wait);
        String dim =
                "create table sink_redis(name varchar, level varchar, age varchar) with ( "
                        + sigleWith()
                        + " 'ttl.on.time'='"
                        + localTime.toString()
                        + "', '"
                        + REDIS_COMMAND
                        + "'='"
                        + RedisCommand.HSET
                        + "' )";

        tEnv.executeSql(dim);
        String sql = " insert into sink_redis select * from (values ('1', '11.3', '10.3'))";
        TableResult tableResult = tEnv.executeSql(sql);
        tableResult.getJobClient().get().getJobExecutionResult().get();
        Preconditions.condition(singleRedisCommands.exists("1") == 1, "");
        Thread.sleep(10 * 1000);
        Preconditions.condition(singleRedisCommands.exists("1") == 0, "");
    }

    @Test
    public void testSinkValueWithExpireOnKeyPresent() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        singleRedisCommands.del("test_hash");
        String ddl =
                "create table source_table(uid VARCHAR) with ('connector'='datagen',"
                        + "'rows-per-second'='1', "
                        + "'fields.uid.kind'='sequence', 'fields.uid.start'='1', 'fields.uid.end'='1')";
        tEnv.executeSql(ddl);

        String dim =
                "create table sink_redis(name varchar, level varchar, age varchar) with ( "
                        + sigleWith()
                        + " 'ttl'='8', 'ttl.key.not.absent'='true', '"
                        + REDIS_COMMAND
                        + "'='"
                        + RedisCommand.HSET
                        + "' )";

        tEnv.executeSql(dim);
        String sql = " insert into sink_redis select 'test_hash', '1', uid from source_table";
        TableResult tableResult = tEnv.executeSql(sql);
        tableResult.getJobClient().get().getJobExecutionResult().get();
        Preconditions.condition(singleRedisCommands.exists("test_hash") == 1, "");
        Thread.sleep(10 * 1000);
        Preconditions.condition(singleRedisCommands.exists("test_hash") == 0, "");
    }
}
