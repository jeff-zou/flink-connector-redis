package org.apache.flink.streaming.connectors.redis.table;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.common.config.RedisOptions;
import org.apache.flink.streaming.connectors.redis.common.hanlder.RedisHandlerServices;
import org.apache.flink.streaming.connectors.redis.common.hanlder.RedisMapperHandler;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.apache.flink.streaming.connectors.redis.descriptor.RedisValidator.*;
import static org.apache.flink.streaming.connectors.redis.descriptor.RedisValidator.REDIS_CLUSTER;

/**
 * Created by jeff.zou on 2020/9/10.
 */
public class SQLInsertTest {

    public static final String CLUSTERNODES = "10.11.80.147:7000,10.11.80.147:7001,10.11.80.147:8000,10.11.80.147:8001,10.11.80.147:9000,10.11.80.147:9001";

    @Test
    public void testNoPrimaryKeyInsertSQL() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, environmentSettings);

        String ddl = "create table sink_redis(username VARCHAR, passport VARCHAR) with ( 'connector'='redis', " +
                "'host'='10.11.80.147','port'='7001', 'redis-mode'='single','password'='******','key-column'='username','value-column'='passport','" +
                REDIS_COMMAND + "'='" + RedisCommand.SET + "')" ;

        tEnv.executeSql(ddl);
        String sql = " insert into sink_redis select * from (values ('test', 'test11'))";
        TableResult tableResult = tEnv.executeSql(sql);
        tableResult.getJobClient().get()
                .getJobExecutionResult()
                .get();
        System.out.println(sql);
    }


    @Test
    public void testSingleInsertHashClusterSQL() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, environmentSettings);

        String ddl = "create table sink_redis(username VARCHAR, level varchar, age varchar) with ( 'connector'='redis', " +
                "'cluster-nodes'='" + CLUSTERNODES + "','redis-mode'='cluster','field-column'='level', 'key-column'='username', 'put-if-absent'='true'," +
                " 'value-column'='age', 'password'='******','" +
                REDIS_COMMAND + "'='" + RedisCommand.HSET + "', 'maxIdle'='2', 'minIdle'='1'  )" ;

        tEnv.executeSql(ddl);
        String sql = " insert into sink_redis select * from (values ('test_hash', '3', '15'))";
        TableResult tableResult = tEnv.executeSql(sql);
        tableResult.getJobClient().get()
                .getJobExecutionResult()
                .get();
        System.out.println(sql);
    }


}