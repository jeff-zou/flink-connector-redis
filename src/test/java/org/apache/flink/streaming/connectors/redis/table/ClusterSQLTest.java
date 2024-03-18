package org.apache.flink.streaming.connectors.redis.table;

import static org.apache.flink.streaming.connectors.redis.config.RedisValidator.REDIS_COMMAND;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.command.RedisCommand;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.jupiter.api.Test;

/** Created by jeff.zou on 2020/9/10. */
public class ClusterSQLTest {

    @Test
    public void testSentinelSQL() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings environmentSettings =
                EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, environmentSettings);

        String ddl =
                "create table sink_redis(username VARCHAR, passport time(3)) with ( 'connector'='redis', "
                        + "'master.name'='mymaster','sentinels.info'='10.11.0.1:26379,10.11.0.2:26379,10.11.0.3:26379', 'redis-mode'='sentinel'"
                        + ",'password'='abc123','sentinels.password'='abc123','"
                        + REDIS_COMMAND
                        + "'='"
                        + RedisCommand.SET
                        + "')";

        tEnv.executeSql(ddl);
        String sql =
                " insert into sink_redis select * from (values ('test_time', time '04:04:00'))";
        TableResult tableResult = tEnv.executeSql(sql);
        tableResult.getJobClient().get().getJobExecutionResult().get();
    }

    @Test
    public void testClusterSQL() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings environmentSettings =
                EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, environmentSettings);

        String ddl =
                "create table sink_redis(username VARCHAR, passport time(3)) with ( 'connector'='redis', "
                        + "'cluster-nodes'='10.11.0.1:7000,10.11.0.1:7001,10.11.0.1:8000,10.11.0.1:8001,10.11.0.1:9000,10.11.0.1:9001', "
                        + " 'redis-mode'='cluster' ,'password'='abc123','"
                        + REDIS_COMMAND
                        + "'='"
                        + RedisCommand.SET
                        + "')";

        tEnv.executeSql(ddl);
        String sql =
                " insert into sink_redis select * from (values ('test_time', time '04:04:00'))";
        TableResult tableResult = tEnv.executeSql(sql);
        tableResult.getJobClient().get().getJobExecutionResult().get();
    }
}
