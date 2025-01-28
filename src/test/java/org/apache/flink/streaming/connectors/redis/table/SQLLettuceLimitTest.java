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

import static org.apache.flink.streaming.connectors.redis.config.RedisValidator.REDIS_COMMAND;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.command.RedisCommand;
import org.apache.flink.streaming.connectors.redis.table.base.TestRedisConfigBase;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.jupiter.api.Test;
import org.junit.platform.commons.util.Preconditions;

/** Created by Jeff Zou on 2020/9/10. */
public class SQLLettuceLimitTest extends TestRedisConfigBase {

    @Test
    public void testSinkLimitLettucePool() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        singleRedisCommands.del("1");
        String ddl =
                "create table source_table(uid VARCHAR) with ('connector'='datagen',"
                        + "'rows-per-second'='1', "
                        + "'fields.uid.kind'='sequence', 'fields.uid.start'='1', 'fields.uid.end'='10')";
        tEnv.executeSql(ddl);

        String sink =
                "create table sink_redis(name varchar, level varchar, age varchar) with (  "
                        + sigleWith()
                        + "'ttl'='10', '"
                        + REDIS_COMMAND
                        + "'='"
                        + RedisCommand.HSET
                        + "','io.pool.size'='3' ,'event.pool.size'='3', 'sink.parallelism'='2')";
        tEnv.executeSql(sink);
        String sql = " insert into sink_redis select '1', '1', uid from source_table";

        TableResult tableResult = tEnv.executeSql(sql);
        tableResult.getJobClient().get().getJobExecutionResult().get();
        Preconditions.condition(singleRedisCommands.exists("1") == 1, "");
        Thread.sleep(10 * 1000);
        Preconditions.condition(singleRedisCommands.exists("1") == 0, "");
    }

    @Test
    public void testJoinLimitLettucePool() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        singleRedisCommands.del("test_hash", "test_hash2");
        singleRedisCommands.hset("test_hash", "1", "test");
        singleRedisCommands.hset("test_hash", "5", "test");
        String dim =
                "create table dim_table(name varchar, level varchar, age varchar) with ( 'connector'='redis', "
                        + "'host'='"
                        + REDIS_HOST
                        + "','port'='"
                        + REDIS_PORT
                        + "', 'redis-mode'='single','password'='"
                        + REDIS_PASSWORD
                        + "','"
                        + REDIS_COMMAND
                        + "'='"
                        + RedisCommand.HGET
                        + "')";

        String source =
                "create table source_table(username varchar, level varchar, proctime as procTime()) "
                        + "with ('connector'='datagen',  'rows-per-second'='1', "
                        + "'fields.username.kind'='sequence',  'fields.username.start'='1',  'fields.username.end'='9',"
                        + "'fields.level.kind'='sequence',  'fields.level.start'='1',  'fields.level.end'='9'"
                        + ")";

        String sink =
                "create table sink_table(username varchar, level varchar,age varchar) with (  'connector'='redis', "
                        + "'host'='"
                        + REDIS_HOST
                        + "','port'='"
                        + REDIS_PORT
                        + "', 'redis-mode'='single','password'='"
                        + REDIS_PASSWORD
                        + "','"
                        + REDIS_COMMAND
                        + "'='"
                        + RedisCommand.HSET
                        + "' )";
        tEnv.executeSql(source);
        tEnv.executeSql(dim);
        tEnv.executeSql(sink);

        String sql =
                " insert into sink_table "
                        + " select 'test_hash2', s.level, d.age from source_table s"
                        + "  left join dim_table for system_time as of s.proctime as d "
                        + " on d.name = 'test_hash' and d.level = s.level";
        TableResult tableResult = tEnv.executeSql(sql);
        tableResult.getJobClient().get().getJobExecutionResult().get();
        System.out.println(sql);

        Preconditions.condition(singleRedisCommands.hget("test_hash2", "1").equals("test"), "");
        Preconditions.condition(singleRedisCommands.hget("test_hash2", "2") == "", "");
        Preconditions.condition(singleRedisCommands.hget("test_hash2", "5").equals("test"), "");
    }
}
