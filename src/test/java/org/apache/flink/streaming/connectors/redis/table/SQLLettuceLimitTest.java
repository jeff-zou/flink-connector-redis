package org.apache.flink.streaming.connectors.redis.table;

import static org.apache.flink.streaming.connectors.redis.descriptor.RedisValidator.REDIS_COMMAND;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.TestRedisConfigBase;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.jupiter.api.Test;
import org.junit.platform.commons.util.Preconditions;

/** Created by jeff.zou on 2020/9/10. */
public class SQLLettuceLimitTest extends TestRedisConfigBase {
    @Test
    public void testSinkLimitLettucePool() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        env.setParallelism(1);
        String ddl =
                "create table source_table(uid VARCHAR) with ('connector'='datagen',"
                        + "'rows-per-second'='1', "
                        + "'fields.uid.kind'='sequence', 'fields.uid.start'='1', 'fields.uid.end'='10')";
        tEnv.executeSql(ddl);

        String sink =
                "create table sink_redis(name varchar, level varchar, age varchar) with (  "
                        + SQLWithUtil.sigleWith()
                        + "'ttl'='10', '"
                        + REDIS_COMMAND
                        + "'='"
                        + RedisCommand.HSET
                        + "','io.pool.size'='3' ,'event.pool.size'='3', 'sink.parallelism'='2')";
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
    public void testJoinLimitLettucePool() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings environmentSettings =
                EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, environmentSettings);

        clusterCommands.hset("1", "1", "test");
        clusterCommands.hset("5", "5", "test");
        String dim =
                "create table dim_table(name varchar, level varchar, age varchar) with ( 'connector'='redis', "
                        + "'cluster-nodes'='"
                        + CLUSTERNODES
                        + "','redis-mode'='cluster', 'password'='"
                        + CLUSTER_PASSWORD
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
                "create table sink_table(username varchar, level varchar,age varchar) with ( 'connector'='redis', "
                        + "'cluster-nodes'='"
                        + CLUSTERNODES
                        + "','redis-mode'='cluster', 'password'='"
                        + CLUSTER_PASSWORD
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
                        + " select concat_ws('_', s.username, s.level), s.level, d.age from source_table s"
                        + "  left join dim_table for system_time as of s.proctime as d "
                        + " on d.name = s.username and d.level = s.level";
        TableResult tableResult = tEnv.executeSql(sql);
        tableResult.getJobClient().get().getJobExecutionResult().get();
        System.out.println(sql);

        Preconditions.condition(clusterCommands.hget("1_1", "1").equals("test"), "");
        Preconditions.condition(clusterCommands.hget("2_2", "2") == "", "");
        Preconditions.condition(clusterCommands.hget("5_5", "5").equals("test"), "");
    }
}
