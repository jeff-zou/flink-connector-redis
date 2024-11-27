package org.apache.flink.streaming.connectors.redis.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import org.junit.Test;

import static org.apache.flink.streaming.connectors.redis.descriptor.RedisValidator.REDIS_COMMAND;

/** Created by jeff.zou on 2020/9/10. */
public class SQLTest {

    public static final String REDIS_HOST = "10.11.69.176";
    public static final int REDIS_PORT = 6379;
    public static final String REDIS_PASSWORD = "***";

    public static final String CLUSTER_PASSWORD = "***";
    public static final String CLUSTERNODES =
            "10.11.80.147:7000,10.11.80.147:7001,10.11.80.147:8000,10.11.80.147:8001,10.11.80.147:9000,10.11.80.147:9001";

    @Test
    public void testNoPrimaryKeyInsertSQL() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings environmentSettings =
                EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, environmentSettings);

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
        String sql = " insert into sink_redis select * from (values ('10', time '04:04:00'))";
        TableResult tableResult = tEnv.executeSql(sql);
        tableResult.getJobClient().get().getJobExecutionResult().get();
        System.out.println(sql);
    }

    @Test
    public void testSingleInsertHashClusterSQL() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings environmentSettings =
                EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, environmentSettings);
        env.setParallelism(1);

        String ddl =
                "create table sink_redis(username varchar, level varchar, age varchar) with ( 'connector'='redis', "
                        + "'cluster-nodes'='"
                        + CLUSTERNODES
                        + "','redis-mode'='cluster', 'password'='"
                        + CLUSTER_PASSWORD
                        + "','"
                        + REDIS_COMMAND
                        + "'='"
                        + RedisCommand.HSET
                        + "', 'maxIdle'='2', 'minIdle'='1'  )";

        tEnv.executeSql(ddl);
        String sql = " insert into sink_redis select * from (values ('3', '3', '18'))";
        TableResult tableResult = tEnv.executeSql(sql);
        tableResult.getJobClient().get().getJobExecutionResult().get();
        System.out.println(sql);
    }

    @Test
    public void testHgetSQL() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings environmentSettings =
                EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, environmentSettings);

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
                        + "', 'maxIdle'='2', 'minIdle'='1', 'lookup.cache.max-rows'='10', 'lookup.cache.ttl'='10', 'lookup.max-retries'='3'  )";

        String source =
                "create table source_table(username varchar, level varchar, proctime as procTime()) "
                        + "with ('connector'='datagen',  'rows-per-second'='1', "
                        + "'fields.username.kind'='sequence',  'fields.username.start'='1',  'fields.username.end'='9',"
                        + "'fields.level.kind'='sequence',  'fields.level.start'='1',  'fields.level.end'='9'"
                        + ")";

        String sink =
                "create table sink_table(username varchar, level varchar,age varchar) with ('connector'='print')";

        tEnv.executeSql(source);
        tEnv.executeSql(dim);
        tEnv.executeSql(sink);

        String sql =
                " insert into sink_table "
                        + " select s.username, s.level, concat_ws('_', d.name, d.age) from source_table s"
                        + "  left join dim_table for system_time as of s.proctime as d "
                        + " on d.name = s.username and d.level = s.level";
        TableResult tableResult = tEnv.executeSql(sql);
        tableResult.getJobClient().get().getJobExecutionResult().get();
        System.out.println(sql);
    }

    @Test
    public void testGetSQL() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        EnvironmentSettings environmentSettings =
                EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, environmentSettings);

        String dim =
                "create table dim_table(name varchar, login_time time(3) ) with ( 'connector'='redis', "
                        + "'cluster-nodes'='"
                        + CLUSTERNODES
                        + "','redis-mode'='cluster', 'password'='"
                        + CLUSTER_PASSWORD
                        + "','"
                        + REDIS_COMMAND
                        + "'='"
                        + RedisCommand.GET
                        + "', 'maxIdle'='2', 'minIdle'='1', 'lookup.cache.max-rows'='10', 'lookup.cache.ttl'='10', 'lookup.max-retries'='3'  )";

        String source =
                "create table source_table(username varchar, level varchar, proctime as procTime()) "
                        + "with ('connector'='datagen',  'rows-per-second'='1', "
                        + "'fields.username.kind'='sequence',  'fields.username.start'='10',  'fields.username.end'='15',"
                        + "'fields.level.kind'='sequence',  'fields.level.start'='10',  'fields.level.end'='15'"
                        + ")";

        String sink =
                "create table sink_table(username varchar, level varchar,login_time time(3)) with ('connector'='print')";

        tEnv.executeSql(source);
        tEnv.executeSql(dim);
        tEnv.executeSql(sink);

        String sql =
                " insert into sink_table "
                        + " select s.username, s.level,  d.login_time from source_table s"
                        + "  left join dim_table for system_time as of s.proctime as d "
                        + " on d.name = s.username";
        TableResult tableResult = tEnv.executeSql(sql);
        tableResult.getJobClient().get().getJobExecutionResult().get();
        System.out.println(sql);
    }

    @Test
    public void testGoupSQL() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings environmentSettings =
                EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, environmentSettings);

        String source =
                "create table source_table(username varchar, level varchar, proctime as procTime()) "
                        + "with ('connector'='datagen',  'rows-per-second'='1', "
                        + "'fields.username.kind'='sequence',  'fields.username.start'='20',  'fields.username.end'='25',"
                        + "'fields.level.kind'='sequence',  'fields.level.start'='20',  'fields.level.end'='25'"
                        + ")";

        String ddl =
                "create table sink_redis(username VARCHAR, username2 VARCHAR, primary key(username) not enforced ) with ( 'connector'='redis', "
                        + "'cluster-nodes'='"
                        + CLUSTERNODES
                        + "','redis-mode'='cluster','password'='"
                        + CLUSTER_PASSWORD
                        + "','"
                        + REDIS_COMMAND
                        + "'='"
                        + RedisCommand.SET
                        + "')";
        tEnv.executeSql(source);
        tEnv.executeSql(ddl);
        env.disableOperatorChaining();
        String sql = " insert into sink_redis select username, username from source_table";
        TableResult tableResult = tEnv.executeSql(sql);
        tableResult.getJobClient().get().getJobExecutionResult().get();
        System.out.println(sql);
    }

    @Test
    public void testDel() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        String ddl =
                "create table redis_sink(redis_key varchar) with('connector'='redis',"
                        + "'cluster-nodes'='"
                        + CLUSTERNODES
                        + "','redis-mode'='cluster','password'='"
                        + CLUSTER_PASSWORD
                        + "','"
                        + REDIS_COMMAND
                        + "'='"
                        + RedisCommand.DEL
                        + "') ";
        tEnv.executeSql(ddl);
        TableResult tableResult =
                tEnv.executeSql("insert into redis_sink select * from (values('20'))");
        tableResult.getJobClient().get().getJobExecutionResult().get();
    }

    @Test
    public void testSRem() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        String ddl =
                "create table redis_sink(redis_key varchar, redis_member varchar) with('connector'='redis',"
                        + "'cluster-nodes'='"
                        + CLUSTERNODES
                        + "','redis-mode'='cluster','password'='"
                        + CLUSTER_PASSWORD
                        + "','"
                        + REDIS_COMMAND
                        + "'='"
                        + RedisCommand.SREM
                        + "') ";
        tEnv.executeSql(ddl);
        TableResult tableResult =
                tEnv.executeSql(
                        "insert into redis_sink select * from (values('s', 'huawei__1056640000002420'))");
        tableResult.getJobClient().get().getJobExecutionResult().get();
    }

    @Test
    public void testHIncryByFloat() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

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
        String sql = " insert into sink_redis select * from (values ('11', '12', 10))";
        TableResult tableResult = tEnv.executeSql(sql);
        tableResult.getJobClient().get().getJobExecutionResult().get();
        System.out.println(sql);
    }

    @Test
    public void testHDel() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        String ddl =
                "create table sink_redis(username VARCHAR, level varchar, score double) with ( 'connector'='redis', "
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
                        + "')";

        tEnv.executeSql(ddl);
        String sql = " insert into sink_redis select * from (values ('11', '12', 10.3))";
        TableResult tableResult = tEnv.executeSql(sql);
        tableResult.getJobClient().get().getJobExecutionResult().get();
        System.out.println(sql);
    }

    @Test
    public void testSinkValueFrom() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

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
        String sql = " insert into sink_redis select * from (values ('1', 11.3, 10.3))";
        TableResult tableResult = tEnv.executeSql(sql);
        tableResult.getJobClient().get().getJobExecutionResult().get();
        System.out.println(sql);
    }

    @Test
    public void testMultiFieldLeftJoinForString() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

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
        System.out.println(ddl);

        // init data in redis
        String sql = " insert into sink_redis select * from (values ('1', 10.3, 10.1))";
        tEnv.executeSql(sql);
        System.out.println(sql);

        // create join table
        ddl =
                "create table join_table with ('command'='get', 'value.data.structure'='row') like sink_redis";
        tEnv.executeSql(ddl);
        System.out.println(ddl);

        // create result table
        ddl =
                "create table result_table(uid VARCHAR, username VARCHAR, score double, score2 double) with ('connector'='print')";
        tEnv.executeSql(ddl);
        System.out.println(ddl);

        // create source table
        ddl =
                "create table source_table(uid VARCHAR, username VARCHAR, proc_time as procTime()) with ('connector'='datagen', 'fields.uid.kind'='sequence', 'fields.uid.start'='1', 'fields.uid.end'='2')";
        tEnv.executeSql(ddl);
        System.out.println(ddl);

        sql =
                "insert into result_table select s.uid, s.username, j.score, j.score2 from source_table as s join join_table for system_time as of s.proc_time as j  on j.uid = s.uid ";
        System.out.println(sql);
        TableResult tableResult = tEnv.executeSql(sql);
        tableResult.getJobClient().get().getJobExecutionResult().get();
    }

    @Test
    public void testMultiFieldLeftJoinForMap() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

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
        System.out.println(ddl);

        // init data in redis
        String sql = " insert into sink_redis select * from (values ('11', '11', 10.3, 10.1))";
        tEnv.executeSql(sql);
        System.out.println(sql);

        // create join table
        ddl =
                "create table join_table with ('command'='hget', 'value.data.structure'='row') like sink_redis";
        tEnv.executeSql(ddl);
        System.out.println(ddl);

        // create result table
        ddl =
                "create table result_table(uid VARCHAR, username VARCHAR, score double, score2 double) with ('connector'='print')";
        tEnv.executeSql(ddl);
        System.out.println(ddl);

        // create source table
        ddl =
                "create table source_table(uid VARCHAR, level varchar, username VARCHAR, proc_time as procTime()) with ('connector'='datagen', 'fields.uid.kind'='sequence', 'fields.uid.start'='10', 'fields.uid.end'='12', 'fields.level.kind'='sequence', 'fields.level.start'='10', 'fields.level.end'='12')";
        tEnv.executeSql(ddl);
        System.out.println(ddl);

        sql =
                "insert into result_table select s.uid, s.username, j.score, j.score2 from source_table as s join join_table for system_time as of s.proc_time as j  on j.uid = s.uid and j.level = s.level";
        System.out.println(sql);
        TableResult tableResult = tEnv.executeSql(sql);
        tableResult.getJobClient().get().getJobExecutionResult().get();
    }

    @Test
    public void testSentinelSQL() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings environmentSettings =
                EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, environmentSettings);

        String ddl =
                "create table sink_redis(username VARCHAR, passport time(3)) with ( 'connector'='redis', "
                        + "'master.name'='mymaster','sentinels.info'='10.11.96.185:26379,10.11.96.186:26379,10.11.96.187:26379', 'redis-mode'='sentinel'"
                        + ",'password'='******','sentinels.password'='******','"
                        + REDIS_COMMAND
                        + "'='"
                        + RedisCommand.SET
                        + "')";

        tEnv.executeSql(ddl);
        String sql =
                " insert into sink_redis select * from (values ('test_time', time '04:14:00'))";
        TableResult tableResult = tEnv.executeSql(sql);
        tableResult.getJobClient().get().getJobExecutionResult().get();
    }
}
