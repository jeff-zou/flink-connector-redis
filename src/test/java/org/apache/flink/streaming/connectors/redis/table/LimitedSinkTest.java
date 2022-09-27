package org.apache.flink.streaming.connectors.redis.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import org.junit.Test;

import static org.apache.flink.streaming.connectors.redis.descriptor.RedisValidator.REDIS_COMMAND;
import static org.apache.flink.streaming.connectors.redis.table.SQLTest.REDIS_HOST;
import static org.apache.flink.streaming.connectors.redis.table.SQLTest.REDIS_PASSWORD;
import static org.apache.flink.streaming.connectors.redis.table.SQLTest.REDIS_PORT;

/**
 * @Author: Jeff Zou @Date: 2022/9/27 15:08
 */
public class LimitedSinkTest {

    @Test
    public void testLimitedSink() throws Exception {
        String sink =
                "create table sink_redis(key_name varchar, user_name VARCHAR, passport varchar) with ( 'connector'='redis', "
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
                        + "', 'sink.limit'='true', 'sink.limit.max-online'='150000','sink.limit.max-num'='10')";

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.executeSql(sink);

        String source =
                "create table source_table (user_name VARCHAR, passport varchar) with ('connector'= 'datagen','rows-per-second'='1',"
                        + " 'fields.user_name.kind'='sequence', 'fields.user_name.start'='0', 'fields.user_name.end'='100',"
                        + " 'fields.passport.kind'='sequence',  'fields.passport.start'='0', 'fields.passport.end'='100')";
        tEnv.executeSql(source);

        tEnv.executeSql(
                        "insert into sink_redis select 'sink_limit_test', user_name, passport from source_table ")
                .getJobClient()
                .get()
                .getJobExecutionResult()
                .get();
    }
}
