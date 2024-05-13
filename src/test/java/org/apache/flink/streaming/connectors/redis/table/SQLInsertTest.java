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

import io.lettuce.core.Range;

import static org.apache.flink.streaming.connectors.redis.config.RedisValidator.REDIS_COMMAND;

/** Created by jeff.zou on 2020/9/10. */
public class SQLInsertTest extends TestRedisConfigBase {

    @Test
    public void testSetSQL() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        String ddl =
                "create table sink_redis(username VARCHAR, passport time(3)) with ( 'connector'='redis', "
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
        String sql =
                " insert into sink_redis select * from (values ('test_time', time '04:04:00'))";
        TableResult tableResult = tEnv.executeSql(sql);
        tableResult.getJobClient().get().getJobExecutionResult().get();
        Preconditions.condition(singleRedisCommands.get("test_time").equals("14640000"), "");
    }

    @Test
    public void testHsetSQL() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        String ddl =
                "create table sink_redis(username varchar, level varchar, age varchar) with ( 'connector'='redis', "
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
                        + "',  'minIdle'='1'  )";

        tEnv.executeSql(ddl);
        String sql = " insert into sink_redis select * from (values ('test_hash', '3', '18'))";
        TableResult tableResult = tEnv.executeSql(sql);
        tableResult.getJobClient().get().getJobExecutionResult().get();
        Preconditions.condition(singleRedisCommands.hget("test_hash", "3").equals("18"), "");
    }

    @Test
    public void testHmsetSQL() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        String ddl =
                "create table sink_redis(username varchar, level varchar, age varchar, fieldkey varchar, fieldvalue varchar) with ( 'connector'='redis', "
                        + "'host'='"
                        + REDIS_HOST
                        + "','port'='"
                        + REDIS_PORT
                        + "', 'redis-mode'='single','password'='"
                        + REDIS_PASSWORD
                        + "','"
                        + REDIS_COMMAND
                        + "'='"
                        + RedisCommand.HMSET
                        + "',  'minIdle'='1'  )";

        tEnv.executeSql(ddl);
        String sql = " insert into sink_redis select * from (values ('test_hash', '3', '18', 'city', '广州'))";
        TableResult tableResult = tEnv.executeSql(sql);
        tableResult.getJobClient().get().getJobExecutionResult().get();
        Preconditions.condition(singleRedisCommands.hget("test_hash", "city").equals("广州"), "");
    }

    @Test
    public void testDel() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        singleRedisCommands.set("testDel", "20");

        String ddl =
                "create table redis_sink(redis_key varchar) with('connector'='redis', "
                        + "'host'='"
                        + REDIS_HOST
                        + "','port'='"
                        + REDIS_PORT
                        + "', 'redis-mode'='single','password'='"
                        + REDIS_PASSWORD
                        + "','"
                        + REDIS_COMMAND
                        + "'='"
                        + RedisCommand.DEL
                        + "') ";
        tEnv.executeSql(ddl);
        TableResult tableResult =
                tEnv.executeSql("insert into redis_sink select * from (values('testDel'))");
        tableResult.getJobClient().get().getJobExecutionResult().get();
        Preconditions.condition(singleRedisCommands.get("testDel") == null, "");
    }

    @Test
    public void testSRem() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        singleRedisCommands.del("set");
        singleRedisCommands.sadd("set", "test1", "test2");
        Preconditions.condition(singleRedisCommands.sismember("set", "test2"), "");
        String ddl =
                "create table redis_sink(redis_key varchar, redis_member varchar) with('connector'='redis', "
                        + "'host'='"
                        + REDIS_HOST
                        + "','port'='"
                        + REDIS_PORT
                        + "', 'redis-mode'='single','password'='"
                        + REDIS_PASSWORD
                        + "','"
                        + REDIS_COMMAND
                        + "'='"
                        + RedisCommand.SREM
                        + "') ";
        tEnv.executeSql(ddl);
        TableResult tableResult =
                tEnv.executeSql("insert into redis_sink select * from (values('set', 'test2'))");
        tableResult.getJobClient().get().getJobExecutionResult().get();
        Preconditions.condition(singleRedisCommands.sismember("set", "test2") == false, "");
    }

    @Test
    public void testHdel() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        singleRedisCommands.del("test_hash");
        singleRedisCommands.hset("test_hash", "test1", "test2");
        Preconditions.condition(singleRedisCommands.hget("test_hash", "test1").equals("test2"), "");
        String ddl =
                "create table redis_sink(redis_key varchar, redis_member varchar) with('connector'='redis', "
                        + "'host'='"
                        + REDIS_HOST
                        + "','port'='"
                        + REDIS_PORT
                        + "', 'redis-mode'='single','password'='"
                        + REDIS_PASSWORD
                        + "','"
                        + REDIS_COMMAND
                        + "'='"
                        + RedisCommand.HDEL
                        + "') ";
        tEnv.executeSql(ddl);
        TableResult tableResult =
                tEnv.executeSql(
                        "insert into redis_sink select * from (values('test_hash', 'test1'))");
        tableResult.getJobClient().get().getJobExecutionResult().get();
        Preconditions.condition(singleRedisCommands.hget("test_hash", "test1") == null, "");
    }

    @Test
    public void testHIncryBy() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        singleRedisCommands.del("test_hash");
        singleRedisCommands.hset("test_hash", "12", "1");
        Preconditions.condition(singleRedisCommands.hget("test_hash", "12").equals("1"), "");
        String ddl =
                "create table sink_redis(username VARCHAR, level varchar, score int) with ( 'connector'='redis', "
                        + "'host'='"
                        + REDIS_HOST
                        + "','port'='"
                        + REDIS_PORT
                        + "', 'redis-mode'='single','password'='"
                        + REDIS_PASSWORD
                        + "','"
                        + REDIS_COMMAND
                        + "'='"
                        + RedisCommand.HINCRBY
                        + "')";

        tEnv.executeSql(ddl);
        String sql = " insert into sink_redis select * from (values ('test_hash', '12', 10))";
        TableResult tableResult = tEnv.executeSql(sql);
        tableResult.getJobClient().get().getJobExecutionResult().get();
        Preconditions.condition(singleRedisCommands.hget("test_hash", "12").equals("11"), "");
    }

    @Test
    public void testHIncryByFloat() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        singleRedisCommands.del("test_hash");
        singleRedisCommands.hset("test_hash", "12", "1");
        Preconditions.condition(singleRedisCommands.hget("test_hash", "12").equals("1"), "");
        String ddl =
                "create table sink_redis(username VARCHAR, level varchar, score float) with ( 'connector'='redis', "
                        + "'host'='"
                        + REDIS_HOST
                        + "','port'='"
                        + REDIS_PORT
                        + "', 'redis-mode'='single','password'='"
                        + REDIS_PASSWORD
                        + "','"
                        + REDIS_COMMAND
                        + "'='"
                        + RedisCommand.HINCRBYFLOAT
                        + "')";

        tEnv.executeSql(ddl);
        String sql = " insert into sink_redis select * from (values ('test_hash', '12', 10.1))";
        TableResult tableResult = tEnv.executeSql(sql);
        tableResult.getJobClient().get().getJobExecutionResult().get();
        Preconditions.condition(singleRedisCommands.hget("test_hash", "12").equals("11.1"), "");
    }

    @Test
    public void testSinkValueFrom() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        singleRedisCommands.del("test");
        String ddl =
                "create table sink_redis(username VARCHAR, score double, score2 double) with ( 'connector'='redis', "
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
        String sql = " insert into sink_redis select * from (values ('test', 11.3, 10.3))";
        TableResult tableResult = tEnv.executeSql(sql);
        tableResult.getJobClient().get().getJobExecutionResult().get();
        String s =
                new StringBuilder()
                        .append("test")
                        .append(RedisDynamicTableFactory.CACHE_SEPERATOR)
                        .append("11.3")
                        .append(RedisDynamicTableFactory.CACHE_SEPERATOR)
                        .append("10.3")
                        .toString();
        Preconditions.condition(singleRedisCommands.get("test").equals(s), "");
    }

    @Test
    public void testIncryBy() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        singleRedisCommands.del("testIncryBy");
        String dim =
                "create table sink_redis(name varchar, level bigint) with ( 'connector'='redis', "
                        + "'host'='"
                        + REDIS_HOST
                        + "','port'='"
                        + REDIS_PORT
                        + "', 'redis-mode'='single','password'='"
                        + REDIS_PASSWORD
                        + "','"
                        + REDIS_COMMAND
                        + "'='"
                        + RedisCommand.INCRBY
                        + "' )";

        tEnv.executeSql(dim);
        String sql = " insert into sink_redis select * from (values ('testIncryBy', 1))";
        tEnv.executeSql(sql);
        TableResult tableResult = tEnv.executeSql(sql);
        tableResult.getJobClient().get().getJobExecutionResult().get();

        Preconditions.condition(singleRedisCommands.get("testIncryBy").toString().equals("2"), "");
    }

    @Test
    public void testIncryBy2() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        singleRedisCommands.del("testIncryBy");
        singleRedisCommands.incrby("testIncryBy", 1);
        String dim =
                "create table sink_redis(name varchar, level bigint) with ( 'connector'='redis', "
                        + "'host'='"
                        + REDIS_HOST
                        + "','port'='"
                        + REDIS_PORT
                        + "', 'redis-mode'='single','password'='"
                        + REDIS_PASSWORD
                        + "','"
                        + REDIS_COMMAND
                        + "'='"
                        + RedisCommand.INCRBY
                        + "' )";

        tEnv.executeSql(dim);
        String sql = " insert into sink_redis select * from (values ('testIncryBy', 3))";
        TableResult tableResult = tEnv.executeSql(sql);
        tableResult.getJobClient().get().getJobExecutionResult().get();

        Preconditions.condition(singleRedisCommands.get("testIncryBy").toString().equals("4"), "");
    }

    @Test
    public void testSetIfAbsent() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings environmentSettings =
                EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, environmentSettings);
        singleRedisCommands.del("test_time");
        singleRedisCommands.set("test_time", "14640000");
        String ddl =
                "create table sink_redis(username VARCHAR, passport VARCHAR) with ( 'connector'='redis', "
                        + "'host'='"
                        + REDIS_HOST
                        + "','port'='"
                        + REDIS_PORT
                        + "', 'redis-mode'='single','password'='"
                        + REDIS_PASSWORD
                        + "', 'set.if.absent'='true"
                        + "','"
                        + REDIS_COMMAND
                        + "'='"
                        + RedisCommand.SET
                        + "')";

        tEnv.executeSql(ddl);
        String sql = " insert into sink_redis select * from (values ('test_time', '0'))";
        TableResult tableResult = tEnv.executeSql(sql);
        tableResult.getJobClient().get().getJobExecutionResult().get();
        Preconditions.condition(singleRedisCommands.get("test_time").equals("14640000"), "");
    }

    @Test
    public void testHsetIfAbsent() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings environmentSettings =
                EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, environmentSettings);
        singleRedisCommands.del("test_time");
        singleRedisCommands.hset("test_time", "test", "14640000");
        String ddl =
                "create table sink_redis(username VARCHAR, passport varchar, my_time time(3)) with ( 'connector'='redis', "
                        + "'host'='"
                        + REDIS_HOST
                        + "','port'='"
                        + REDIS_PORT
                        + "', 'redis-mode'='single','password'='"
                        + REDIS_PASSWORD
                        + "', 'set.if.absent'='true"
                        + "','"
                        + REDIS_COMMAND
                        + "'='"
                        + RedisCommand.HSET
                        + "')";

        tEnv.executeSql(ddl);
        String sql =
                " insert into sink_redis select * from (values ('test_time', 'test', time '05:04:00'))";
        TableResult tableResult = tEnv.executeSql(sql);
        tableResult.getJobClient().get().getJobExecutionResult().get();
        Preconditions.condition(
                singleRedisCommands.hget("test_time", "test").equals("14640000"), "");
    }

    @Test
    public void testZadd() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings environmentSettings =
                EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, environmentSettings);
        singleRedisCommands.del("test_zadd");
        String ddl =
                "create table sink_redis(username VARCHAR, age varchar, passport varchar) with ( 'connector'='redis', "
                        + "'host'='"
                        + REDIS_HOST
                        + "','port'='"
                        + REDIS_PORT
                        + "', 'redis-mode'='single','password'='"
                        + REDIS_PASSWORD
                        + "', 'set.if.absent'='true"
                        + "','"
                        + REDIS_COMMAND
                        + "'='"
                        + RedisCommand.ZADD
                        + "')";

        tEnv.executeSql(ddl);
        String sql = " insert into sink_redis select * from (values ('test_zadd', '100',  'test'))";
        TableResult tableResult = tEnv.executeSql(sql);
        tableResult.getJobClient().get().getJobExecutionResult().get();
        Preconditions.condition(singleRedisCommands.zscore("test_zadd", "test") == 100, "");
    }

    @Test
    public void testZincry() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings environmentSettings =
                EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, environmentSettings);
        singleRedisCommands.del("test_zadd");
        singleRedisCommands.zadd("test_zadd", 100, "test");
        String ddl =
                "create table sink_redis(username VARCHAR, age varchar, passport varchar) with ( 'connector'='redis', "
                        + "'host'='"
                        + REDIS_HOST
                        + "','port'='"
                        + REDIS_PORT
                        + "', 'redis-mode'='single','password'='"
                        + REDIS_PASSWORD
                        + "', 'set.if.absent'='true"
                        + "','"
                        + REDIS_COMMAND
                        + "'='"
                        + RedisCommand.ZINCRBY
                        + "')";

        tEnv.executeSql(ddl);
        String sql = " insert into sink_redis select * from (values ('test_zadd', '100',  'test'))";
        TableResult tableResult = tEnv.executeSql(sql);
        tableResult.getJobClient().get().getJobExecutionResult().get();
        Preconditions.condition(singleRedisCommands.zscore("test_zadd", "test") == 200, "");
    }

    @Test
    public void testZaddAndzremRangeByScore() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings environmentSettings =
                EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, environmentSettings);
        singleRedisCommands.del("test_zadd");
        String datagen = "CREATE TABLE datagen (" +
                "    score INT," +
                "    mm VARCHAR" +
                "  ) WITH (" +
                "    'connector' = 'datagen'," +
                "    'rows-per-second'='1'," +
                "    'fields.score.kind'='sequence'," +
                "    'fields.score.start'='1'," +
                "    'fields.score.end'='10'," +
                "    'fields.mm.length'='10'" +
                "  )";
        String ddl =
                "create table sink_redis(key VARCHAR, score INT, mm VARCHAR, " +
                        "rem_min_score DOUBLE, rem_max_score DOUBLE) with ( " +
                        "'connector'='redis', "
                        + "'host'='"
                        + REDIS_HOST
                        + "','port'='"
                        + REDIS_PORT
                        + "', 'redis-mode'='single','password'='"
                        + REDIS_PASSWORD
                        + "','"
                        + "zset.zremrangeby' = 'SCORE','"
                        + REDIS_COMMAND
                        + "'='"
                        + RedisCommand.ZADD
                        + "')";

        tEnv.executeSql(datagen);
        tEnv.executeSql(ddl);
        String sql = " insert into sink_redis select 'test_zadd' AS key, score, mm, 1 as rem_min_score  ,5 as " +
                "rem_max_score " +
                "from datagen";
        TableResult tableResult = tEnv.executeSql(sql);
        tableResult.getJobClient().get().getJobExecutionResult().get();
        Range<Integer> range = Range.create(1, 5);
        Preconditions.condition(singleRedisCommands.zrangebyscore("test_zadd", range).size() == 0, "");
    }

    @Test
    public void testZaddAndzremRangeByLex() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        EnvironmentSettings environmentSettings =
                EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, environmentSettings);
        singleRedisCommands.del("test_zadd");
        String datagen = "CREATE TABLE datagen (" +
                "    index INT," +
                "    mm VARCHAR" +
                "  ) WITH (" +
                "    'connector' = 'datagen'," +
                "    'rows-per-second'='1'," +
                "    'fields.index.kind'='sequence'," +
                "    'fields.index.start'='1'," +
                "    'fields.index.end'='10'," +
                "    'fields.mm.length'='5'" +
                "  )";
        String ddl =
                "create table sink_redis(key VARCHAR, score INT, mm VARCHAR, " +
                        "rem_begin VARCHAR, rem_end VARCHAR" +
                        ",primary key (key) not ENFORCED" +
                        ") with ( " +
                        "'connector'='redis', "
                        + "'host'='"
                        + REDIS_HOST
                        + "','port'='"
                        + REDIS_PORT
                        + "', 'redis-mode'='single','password'='"
                        + REDIS_PASSWORD
                        + "','"
                        + "zset.zremrangeby' = 'LEX','"
                        + REDIS_COMMAND
                        + "'='"
                        + RedisCommand.ZADD
                        + "')";

        tEnv.executeSql(datagen);
        tEnv.executeSql(ddl);
        String sql = " insert into sink_redis select 'test_zadd' AS key, 100 as score, concat('aa', mm) mm, 'aa' as " +
                "rem_begin ,'bb' as rem_end " +
                "from datagen";
        TableResult tableResult = tEnv.executeSql(sql);
        tableResult.getJobClient().get().getJobExecutionResult().get();
        Range<String> range = Range.create("aa", "bb");
        Preconditions.condition(singleRedisCommands.zrangebylex("test_zadd", range).size() == 0, "");
    }

    @Test
    public void testZaddAndzremRangeByRank() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        EnvironmentSettings environmentSettings =
                EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, environmentSettings);
        singleRedisCommands.del("test_zadd");
        String datagen = "CREATE TABLE datagen (" +
                "    index INT," +
                "    mm VARCHAR" +
                "  ) WITH (" +
                "    'connector' = 'datagen'," +
                "    'rows-per-second'='1'," +
                "    'fields.index.kind'='sequence'," +
                "    'fields.index.start'='1'," +
                "    'fields.index.end'='10'," +
                "    'fields.mm.length'='5'" +
                "  )";
        String ddl =
                "create table sink_redis(key VARCHAR, score INT, mm VARCHAR, " +
                        "rem_begin INT, rem_end INT" +
                        ",primary key (key) not ENFORCED" +
                        ") with ( " +
                        "'connector'='redis', "
                        + "'host'='"
                        + REDIS_HOST
                        + "','port'='"
                        + REDIS_PORT
                        + "', 'redis-mode'='single','password'='"
                        + REDIS_PASSWORD
                        + "','"
                        + "zset.zremrangeby' = 'RANK','"
                        + REDIS_COMMAND
                        + "'='"
                        + RedisCommand.ZADD
                        + "')";

        tEnv.executeSql(datagen);
        tEnv.executeSql(ddl);
        String sql = " insert into sink_redis select 'test_zadd' AS key, 100 as score, concat('aa', mm) mm, 0 as " +
                "rem_begin ,9 as rem_end " +
                "from datagen";
        TableResult tableResult = tEnv.executeSql(sql);
        tableResult.getJobClient().get().getJobExecutionResult().get();
        Preconditions.condition(singleRedisCommands.zrange("test_zadd", 0, -1).size() == 0, "");
    }

}
