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
        singleRedisCommands.set("test", "1");
        String source =
                "create table source_redis(username VARCHAR, passport int) with ( 'connector'='redis', "
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
}
