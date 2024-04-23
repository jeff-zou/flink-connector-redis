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
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.jupiter.api.Test;
import org.junit.platform.commons.util.Preconditions;

import static org.apache.flink.streaming.connectors.redis.config.RedisValidator.REDIS_COMMAND;

/**
 * @author Jeff Zou
 * @date 2024/3/19 17:07
 */
public class SQLJoinTest extends TestRedisConfigBase {

    @Test
    public void testGetSet() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings environmentSettings =
                EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, environmentSettings);
        singleRedisCommands.set("1", "test");

        String source =
                "create table source_table(username varchar, level varchar, proctime as procTime()) "
                        + "with ('connector'='datagen',  'rows-per-second'='1', "
                        + "'fields.username.kind'='sequence',  'fields.username.start'='1',  'fields.username.end'='9',"
                        + "'fields.level.kind'='sequence',  'fields.level.start'='1',  'fields.level.end'='9'"
                        + ")";
        tEnv.executeSql(source);
        String ddl =
                "create table join_redis(username VARCHAR, passport varchar) with ( 'connector'='redis', "
                        + "'host'='"
                        + REDIS_HOST
                        + "','port'='"
                        + REDIS_PORT
                        + "', 'redis-mode'='single','password'='"
                        + REDIS_PASSWORD
                        + "','"
                        + REDIS_COMMAND
                        + "'='"
                        + RedisCommand.GET
                        + "')";

        tEnv.executeSql(ddl);

        tEnv.executeSql(
                "create table sink_table(msg row<request_id varchar , `rank`  varchar>) with ('connector'='print')");

        String sql =
                " insert into sink_table select row('test', b.passport) from  source_table s "
                        + "left join join_redis for system_time as of proctime as b on s.username = b.username";
        TableResult tableResult = tEnv.executeSql(sql);
        tableResult.getJobClient().get().getJobExecutionResult().get();
    }

    @Test
    public void testGetAfterInsert() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        String dim =
                "create table dim_table(name varchar, nickname varchar) with ( 'connector'='redis', "
                        + "'host'='"
                        + REDIS_HOST
                        + "','redis-mode'='single', 'password'='"
                        + REDIS_PASSWORD
                        + "','"
                        + REDIS_COMMAND
                        + "'='"
                        + RedisCommand.GET
                        + "' )";

        String source =
                "create table source_table(name varchar, proctime as procTime()) "
                        + "with ('connector'='datagen',  'rows-per-second'='1', 'fields.name.kind'='sequence',  'fields.name.start'='1',  'fields.name.end'='5' "
                        + ")";

        String sink =
                "create table sink_table(name varchar, nickname varchar) with ( 'connector'='print')";

        tEnv.executeSql(source);
        tEnv.executeSql(dim);
        tEnv.executeSql(sink);

        tEnv.executeSql("create table dim_table_insert with('command'='set') like dim_table");
        TableResult tableResult2 =
                tEnv.executeSql(
                        "insert into dim_table_insert select * from (values( '1', 'test') )");
        tableResult2.getJobClient().get().getJobExecutionResult().get();

        String sql =
                " insert into sink_table "
                        + " select s.name,  d.nickname from source_table s"
                        + "  left join dim_table for system_time as of s.proctime as d "
                        + " on d.name =s.name ";
        TableResult tableResult = tEnv.executeSql(sql);
        tableResult.getJobClient().get().getJobExecutionResult().get();
    }

    @Test
    public void testHGetAfterInsert() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings environmentSettings =
                EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, environmentSettings);
        singleRedisCommands.del("1", "2", "3", "4", "5");
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
                        + "' )";

        String source =
                "create table source_table(name varchar, level varchar, proctime as procTime()) "
                        + "with ('connector'='datagen',  'rows-per-second'='1', 'fields.name.kind'='sequence',  'fields.name.start'='1',  'fields.name.end'='5', "
                        + "'fields.level.kind'='sequence',  'fields.level.start'='1',  'fields.level.end'='5'"
                        + ")";

        String sink =
                "create table sink_table(name varchar, level varchar,age varchar) with ( 'connector'='print')";

        tEnv.executeSql(source);
        tEnv.executeSql(dim);
        tEnv.executeSql(sink);

        tEnv.executeSql("create table dim_table_insert with('command'='hset') like dim_table");
        TableResult tableResult2 =
                tEnv.executeSql(
                        "insert into dim_table_insert select * from (values( '1', '1', 'test') )");
        tableResult2.getJobClient().get().getJobExecutionResult().get();

        String sql =
                " insert into sink_table "
                        + " select s.name,  d.level, d.age from source_table s"
                        + "  left join dim_table for system_time as of s.proctime as d "
                        + " on d.name =s.name and d.level =s.level";
        TableResult tableResult = tEnv.executeSql(sql);
        tableResult.getJobClient().get().getJobExecutionResult().get();
    }

    @Test
    public void testMultiFieldLeftJoinForString() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        singleRedisCommands.del("1", "2", "3", "1_1", "2_2");
        // init data in redis
        String ddl =
                "create table sink_redis(uid VARCHAR, score double, score2 double ) with ( 'connector'='redis', "
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
                        + "', 'value.data.structure'='row')";

        tEnv.executeSql(ddl);

        // init data in redis
        String sql =
                " insert into sink_redis select * from (values ('1', 10.3, 10.1),('2', 10.1, 10.1),('3', 10.3, 10.1))";
        tEnv.executeSql(sql).getJobClient().get().getJobExecutionResult().get();

        // create join table
        ddl =
                "create table join_table with ('command'='get', 'value.data.structure'='row') like sink_redis";
        tEnv.executeSql(ddl);

        // create result table
        ddl =
                "create table result_table(uid VARCHAR, score double) with ('connector'='redis', "
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
                        + "')";
        tEnv.executeSql(ddl);

        // create source table
        ddl =
                "create table source_table(uid VARCHAR, username VARCHAR, proc_time as procTime()) with ('connector'='datagen', 'fields.uid.kind'='sequence', 'fields.uid.start'='1', 'fields.uid.end'='2')";
        tEnv.executeSql(ddl);

        sql =
                "insert into result_table select concat_ws('_', s.uid, s.uid), j.score from source_table as s join join_table for system_time as of s.proc_time as j  on j.uid = s.uid ";
        TableResult tableResult = tEnv.executeSql(sql);
        tableResult.getJobClient().get().getJobExecutionResult().get();
        Preconditions.condition(singleRedisCommands.get("1_1").equals("10.3"), "");
        Preconditions.condition(singleRedisCommands.get("2_2").equals("10.1"), "");
    }

    @Test
    public void testMultiFieldLeftJoinForMap() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        singleRedisCommands.del("test_hash");
        // init data in redis
        String ddl =
                "create table sink_redis(uid VARCHAR, level varchar, score double, score2 double ) with ( 'connector'='redis', "
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
                        + "', 'value.data.structure'='row')";

        tEnv.executeSql(ddl);

        // init data in redis
        String sql =
                " insert into sink_redis select * from (values ('test_hash', '11', 10.3, 10.1))";
        tEnv.executeSql(sql).getJobClient().get();

        // create join table
        ddl =
                "create table join_table with ('command'='hget', 'value.data.structure'='row') like sink_redis";
        tEnv.executeSql(ddl);
        // create result table
        ddl =
                "create table result_table(uid VARCHAR, level VARCHAR, score double) with ('connector'='redis', "
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
                        + "')";
        tEnv.executeSql(ddl);

        // create source table
        ddl =
                "create table source_table(uid VARCHAR, level varchar, username VARCHAR, proc_time as procTime()) with ('connector'='datagen', 'fields.uid.kind'='sequence', 'fields.uid.start'='10', 'fields.uid.end'='12', 'fields.level.kind'='sequence', 'fields.level.start'='10', 'fields.level.end'='12')";
        tEnv.executeSql(ddl);

        sql =
                "insert into result_table select 'test_hash', concat_ws('_', s.level, s.level), j.score "
                        + " from source_table as s join join_table for system_time as of s.proc_time as j  on j.uid = 'test_hash' and j.level = s.level";
        TableResult tableResult = tEnv.executeSql(sql);
        tableResult.getJobClient().get().getJobExecutionResult().get();
        Preconditions.condition(singleRedisCommands.hget("test_hash", "10_10") == null, "");
        Preconditions.condition(singleRedisCommands.hget("test_hash", "11_11").equals("10.3"), "");
        Preconditions.condition(singleRedisCommands.hget("test_hash", "12_12") == null, "");
    }

    @Test
    public void testHgetSQL() throws Exception {
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
                        + "',   'lookup.cache.max-rows'='10', 'lookup.cache.ttl'='10', 'max.retries'='3'  )";

        String source =
                "create table source_table(username varchar, level varchar, proctime as procTime()) "
                        + "with ('connector'='datagen',  'rows-per-second'='1', "
                        + "'fields.username.kind'='sequence',  'fields.username.start'='1',  'fields.username.end'='9',"
                        + "'fields.level.kind'='sequence',  'fields.level.start'='1',  'fields.level.end'='9'"
                        + ")";

        String sink =
                "create table sink_table(username varchar, level varchar,age varchar) with ( 'connector'='redis', "
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

    @Test
    public void testHgetFieldIsNullSQL() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings environmentSettings =
                EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, environmentSettings);
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
                        + "',   'lookup.cache.max-rows'='10', 'lookup.cache.ttl'='10', 'max.retries'='3'  )";

        String source =
                "create table source_table(username varchar, level varchar, proctime as procTime()) "
                        + "with ('connector'='datagen',  'rows-per-second'='1', "
                        + "'fields.username.kind'='sequence',  'fields.username.start'='1',  'fields.username.end'='9',"
                        + "'fields.level.kind'='sequence',  'fields.level.start'='1',  'fields.level.end'='9'"
                        + ")";

        String sink =
                "create table sink_table(username varchar, level varchar,age varchar) with ( 'connector'='redis', "
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

    @Test
    public void testGetSQL() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        singleRedisCommands.del("10", "11", "12", "13", "14", "15");
        singleRedisCommands.set("10", "1800000");
        String dim =
                "create table dim_table(name varchar, login_time time(3) ) with ( 'connector'='redis', "
                        + "'host'='"
                        + REDIS_HOST
                        + "','port'='"
                        + REDIS_PORT
                        + "', 'redis-mode'='single','password'='"
                        + REDIS_PASSWORD
                        + "','"
                        + REDIS_COMMAND
                        + "'='"
                        + RedisCommand.GET
                        + "',   'lookup.cache.max-rows'='10', 'lookup.cache.ttl'='10', 'max.retries'='3'  )";

        String source =
                "create table source_table(username varchar, level varchar, proctime as procTime()) "
                        + "with ('connector'='datagen',  'rows-per-second'='1', "
                        + "'fields.username.kind'='sequence',  'fields.username.start'='10',  'fields.username.end'='15',"
                        + "'fields.level.kind'='sequence',  'fields.level.start'='10',  'fields.level.end'='15'"
                        + ")";

        String sink =
                "create table sink_table(username varchar, level varchar,login_time time(3)) with  ('connector'='redis', "
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
                        + " select 'test_hash', s.level, d.login_time from source_table s"
                        + "  left join dim_table for system_time as of s.proctime as d "
                        + " on d.name = s.username";
        TableResult tableResult = tEnv.executeSql(sql);
        tableResult.getJobClient().get().getJobExecutionResult().get();
        Preconditions.condition(singleRedisCommands.hget("test_hash", "10").equals("1800000"), "");
        Preconditions.condition(singleRedisCommands.hget("test_hash", "11") == "", "");
        Preconditions.condition(singleRedisCommands.hget("test_hash", "12") == "", "");
    }

    @Test
    public void testZscore() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        singleRedisCommands.del("test_sorted_set");
        singleRedisCommands.zadd("test_sorted_set", 1d, "10");
        String join =
                "create table redis_table(username VARCHAR, age double, passport VARCHAR) with ( 'connector'='redis', "
                        + "'host'='"
                        + REDIS_HOST
                        + "','port'='"
                        + REDIS_PORT
                        + "', 'redis-mode'='single', 'password'='"
                        + REDIS_PASSWORD
                        + "','"
                        + REDIS_COMMAND
                        + "'='"
                        + RedisCommand.ZADD
                        + "')";
        tEnv.executeSql(join);

        String source =
                "create table source_table(passport varchar, age double, proctime as procTime()) "
                        + "with ('connector'='datagen',  'rows-per-second'='1', "
                        + "'fields.passport.kind'='sequence',  'fields.passport.start'='10',  'fields.passport.end'='10',"
                        + "'fields.age.kind'='sequence',  'fields.age.start'='10',  'fields.age.end'='10'"
                        + ")";
        tEnv.executeSql(source);

        String sql =
                "insert into redis_table select 'test_sorted_set', j.age + s.age, s.passport from source_table s left join redis_table for system_time as of proctime as j on j.username = 'test_sorted_set' and j.passport = s.passport";
        TableResult tableResult = tEnv.executeSql(sql);
        tableResult.getJobClient().get().getJobExecutionResult().get();
        Preconditions.condition(singleRedisCommands.zscore("test_sorted_set", "10") == 11, "");
    }
}
