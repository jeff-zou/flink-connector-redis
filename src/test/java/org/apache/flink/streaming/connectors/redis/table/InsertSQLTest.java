package org.apache.flink.streaming.connectors.redis.table;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.Test;

import static org.apache.flink.streaming.connectors.redis.descriptor.RedisValidator.REDIS_COMMAND;

/**
 * Created by jeff.zou on 2020/9/10.
 */
public class InsertSQLTest {

    public static final String CLUSTERNODES = "10.11.80.147:7000,10.11.80.147:7001,10.11.80.147:8000,10.11.80.147:8001,10.11.80.147:9000,10.11.80.147:9001";

    @Test
    public void testNoPrimaryKeyInsertSQL() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, environmentSettings);

        String ddl = "create table sink_redis(username VARCHAR, passport VARCHAR) with ( 'connector'='redis', " +
                "'host'='10.11.80.147','port'='7001', 'redis-mode'='single','password'='****','" +
                REDIS_COMMAND + "'='" + RedisCommand.SET + "')" ;

        tEnv.executeSql(ddl);
        String sql = " insert into sink_redis select * from (values ('no_primary_key', 'test11'))";
        TableResult tableResult = tEnv.executeSql(sql);
        tableResult.getJobClient().get()
                .getJobExecutionResult()
                .get();
        System.out.println(sql);
    }

    @Test
    public void testNoPrimaryKeyInsertClusterSQL() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, environmentSettings);

        String ddl = "create table sink_redis(username VARCHAR, passport VARCHAR) with ( 'connector'='redis', " +
                "'cluster-nodes'='" + CLUSTERNODES + "','redis-mode'='cluster', 'password'='****','" +
                 REDIS_COMMAND + "'='" + RedisCommand.SET + "')" ;

        tEnv.executeSql(ddl);
        String sql = " insert into sink_redis select * from (values ('no_primary_key', 'test1'))";
        TableResult tableResult = tEnv.executeSql(sql);
        tableResult.getJobClient().get()
                .getJobExecutionResult()
                .get();
        System.out.println(sql);
    }


    @Test
    public void testSinglePrimaryKeyInsertClusterSQL() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, environmentSettings);

        String ddl = "create table sink_redis( passport VARCHAR, username VARCHAR, primary key (username) not enforced) with ( 'connector'='redis', " +
                "'cluster-nodes'='" + CLUSTERNODES + "','redis-mode'='cluster', 'password'='****','" +
                REDIS_COMMAND + "'='" + RedisCommand.SET + "')" ;

        tEnv.executeSql(ddl);
        String sql = " insert into sink_redis select * from (values ('test2', 'single_primary_key'))";
        TableResult tableResult = tEnv.executeSql(sql);
        tableResult.getJobClient().get()
                .getJobExecutionResult()
                .get();
        System.out.println(sql);
    }

    @Test
    public void testMultiPrimaryKeyInsertClusterSQL() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, environmentSettings);

        String ddl = "create table sink_redis(username VARCHAR, level varchar, age varchar, passport VARCHAR,  primary key (username, level) not enforced) with ( 'connector'='redis', " +
                "'cluster-nodes'='" + CLUSTERNODES + "','redis-mode'='cluster', 'password'='****','" +
                REDIS_COMMAND + "'='" + RedisCommand.SET + "')" ;

        tEnv.executeSql(ddl);
        String sql = " insert into sink_redis select * from (values ('multi_primary_key', '2', '1', 'test3'))";
        TableResult tableResult = tEnv.executeSql(sql);
        tableResult.getJobClient().get()
                .getJobExecutionResult()
                .get();
        System.out.println(sql);
    }


    @Test
    public void testMultiPrimaryKeyInsertHashClusterSQL() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, environmentSettings);

        String ddl = "create table sink_redis(username VARCHAR, level varchar, age varchar, passport VARCHAR,  primary key (username, level) not enforced) with ( 'connector'='redis', " +
                "'cluster-nodes'='" + CLUSTERNODES + "','redis-mode'='cluster','additional-key'='hash_table_test', 'password'='****','" +
                REDIS_COMMAND + "'='" + RedisCommand.HSET + "', 'maxIdle'='2', 'minIdle'='1'  )" ;

        tEnv.executeSql(ddl);
        String sql = " insert into sink_redis select * from (values ('multi_primary_key', '3', '1', 'test3'))";
        TableResult tableResult = tEnv.executeSql(sql);
        tableResult.getJobClient().get()
                .getJobExecutionResult()
                .get();
        System.out.println(sql);
    }



    @Test
    public void testMultiPrimaryKeyMultiValueInsertHashClusterSQL() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, environmentSettings);

        String ddl = "create table sink_redis(username VARCHAR, level varchar, age varchar, passport VARCHAR,  primary key (username, level) not enforced) with ( 'connector'='redis', " +
                "'cluster-nodes'='" + CLUSTERNODES + "','redis-mode'='cluster','additional-key'='hash_table_test', 'password'='***','" +
                REDIS_COMMAND + "'='" + RedisCommand.HSET + "', 'maxIdle'='2', 'minIdle'='1'  )" ;

        tEnv.executeSql(ddl);
        String sql = " insert into sink_redis select * from (values ('multi_primary_value_key', '3', 'age', 'passport'))";
        TableResult tableResult = tEnv.executeSql(sql);
        tableResult.getJobClient().get()
                .getJobExecutionResult()
                .get();
        System.out.println(sql);
    }
}