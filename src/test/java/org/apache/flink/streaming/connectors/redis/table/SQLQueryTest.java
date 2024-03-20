package org.apache.flink.streaming.connectors.redis.table;

import static org.apache.flink.streaming.connectors.redis.config.RedisValidator.REDIS_COMMAND;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.command.RedisCommand;
import org.apache.flink.streaming.connectors.redis.table.base.TestRedisConfigBase;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Preconditions;
import org.junit.jupiter.api.Test;

public class SQLQueryTest extends TestRedisConfigBase {

    @Test
    public void testQuery() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        singleRedisCommands.del("test");
        singleRedisCommands.set("test", "1");
        String source =
                "create table source_redis(username VARCHAR, passport int) with ( 'connector'='redis', "
                        + "'host'='"
                        + REDIS_HOST
                        + "','port'='"
                        + REDIS_PORT
                        + "', 'redis-mode'='single','scan.key'='test', 'password'='"
                        + REDIS_PASSWORD
                        + "','"
                        + REDIS_COMMAND
                        + "'='"
                        + RedisCommand.GET
                        + "')";
        tEnv.executeSql(source);
        String sink =
                "create table sink_table(username varchar, passport int) with ( 'connector'='redis', "
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
        tEnv.executeSql(sink);
        TableResult tableResult =
                tEnv.executeSql(
                        "insert into sink_table select username,passport + 1 from source_redis ");

        tableResult.getJobClient().get().getJobExecutionResult().get();
        Preconditions.checkArgument(singleRedisCommands.get("test").equals("2"));
    }

    @Test
    public void testMapQuery() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        singleRedisCommands.del("test_hash");
        singleRedisCommands.hset("test_hash", "1", "1");
        String source =
                "create table source_redis(username VARCHAR, passport int, age int) with ( 'connector'='redis', "
                        + "'host'='"
                        + REDIS_HOST
                        + "','port'='"
                        + REDIS_PORT
                        + "', 'redis-mode'='single','scan.key'='test_hash', 'scan.addition.key'='1', 'password'='"
                        + REDIS_PASSWORD
                        + "','"
                        + REDIS_COMMAND
                        + "'='"
                        + RedisCommand.HGET
                        + "')";
        tEnv.executeSql(source);
        String sink =
                "create table sink_table(username varchar, passport int, age int) with ( 'connector'='redis', "
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
        tEnv.executeSql(sink);
        TableResult tableResult =
                tEnv.executeSql(
                        "insert into sink_table select username,passport, age + 1 from source_redis ");

        tableResult.getJobClient().get().getJobExecutionResult().get();
        Preconditions.checkArgument(singleRedisCommands.hget("test_hash", "1").equals("2"));
    }

    @Test
    public void testSortedSetQuery() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        singleRedisCommands.del("test_sorted_set");
        singleRedisCommands.zadd("test_sorted_set", 1d, "test");
        String source =
                "create table source_redis(username VARCHAR, age double, passport VARCHAR) with ( 'connector'='redis', "
                        + "'host'='"
                        + REDIS_HOST
                        + "','port'='"
                        + REDIS_PORT
                        + "', 'redis-mode'='single','scan.key'='test_sorted_set', 'scan.addition.key'='test', 'password'='"
                        + REDIS_PASSWORD
                        + "','"
                        + REDIS_COMMAND
                        + "'='"
                        + RedisCommand.ZADD
                        + "')";
        tEnv.executeSql(source);
        String sink =
                "create table sink_table(username varchar, age double, passport VARCHAR) with ( 'connector'='redis', "
                        + "'host'='"
                        + REDIS_HOST
                        + "','port'='"
                        + REDIS_PORT
                        + "', 'redis-mode'='single','password'='"
                        + REDIS_PASSWORD
                        + "','"
                        + REDIS_COMMAND
                        + "'='"
                        + RedisCommand.ZADD
                        + "')";
        tEnv.executeSql(sink);
        TableResult tableResult =
                tEnv.executeSql(
                        "insert into sink_table select username, age + 1 ,passport from source_redis ");

        tableResult.getJobClient().get().getJobExecutionResult().get();
        Preconditions.checkArgument(singleRedisCommands.zscore("test_sorted_set", "test") == 2);
    }

    @Test
    public void testLrangeQuery() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        singleRedisCommands.del("test_list");
        singleRedisCommands.lpush("test_list", "2", "test2");
        String source =
                "create table source_redis(username VARCHAR, passport VARCHAR) with ( 'connector'='redis', "
                        + "'host'='"
                        + REDIS_HOST
                        + "','port'='"
                        + REDIS_PORT
                        + "', 'redis-mode'='single','scan.key'='test_list', 'scan.range.start'='0', 'scan.range.stop'='1', 'password'='"
                        + REDIS_PASSWORD
                        + "','"
                        + REDIS_COMMAND
                        + "'='"
                        + RedisCommand.LPUSH
                        + "')";
        tEnv.executeSql(source);
        String sink =
                "create table sink_table(username varchar, passport VARCHAR) with ( 'connector'='print')";
        tEnv.executeSql(sink);
        TableResult tableResult =
                tEnv.executeSql("insert into sink_table select * from source_redis ");

        tableResult.getJobClient().get().getJobExecutionResult().get();
    }

    @Test
    public void testSrandmemberQuery() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        singleRedisCommands.del("test_set");
        singleRedisCommands.sadd("test_set", "2", "test2");
        String source =
                "create table source_redis(username VARCHAR, passport VARCHAR) with ( 'connector'='redis', "
                        + "'host'='"
                        + REDIS_HOST
                        + "','port'='"
                        + REDIS_PORT
                        + "', 'redis-mode'='single','scan.key'='test_set', 'scan.count'='2', 'password'='"
                        + REDIS_PASSWORD
                        + "','"
                        + REDIS_COMMAND
                        + "'='"
                        + RedisCommand.SADD
                        + "')";
        tEnv.executeSql(source);
        String sink =
                "create table sink_table(username varchar, passport VARCHAR) with ( 'connector'='print')";
        tEnv.executeSql(sink);
        TableResult tableResult =
                tEnv.executeSql("insert into sink_table select * from source_redis ");

        tableResult.getJobClient().get().getJobExecutionResult().get();
    }
}
