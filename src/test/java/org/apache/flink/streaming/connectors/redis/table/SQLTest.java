package org.apache.flink.streaming.connectors.redis.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.TestRedisConfigBase;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import org.junit.jupiter.api.Test;
import org.junit.platform.commons.util.Preconditions;

import java.time.LocalTime;

import static org.apache.flink.streaming.connectors.redis.descriptor.RedisValidator.REDIS_COMMAND;

/** Created by jeff.zou on 2020/9/10. */
public class SQLTest extends TestRedisConfigBase {

    @Test
    public void testNoPrimaryKeyInsertSQL() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings environmentSettings =
                EnvironmentSettings.newInstance().inStreamingMode().build();
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
        String sql =
                " insert into sink_redis select * from (values ('test_time', time '04:04:00'))";
        TableResult tableResult = tEnv.executeSql(sql);
        tableResult.getJobClient().get().getJobExecutionResult().get();
        Preconditions.condition(singleRedisCommands.get("test_time").equals("14640000"), "");
    }

    @Test
    public void testSingleInsertHashClusterSQL() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings environmentSettings =
                EnvironmentSettings.newInstance().inStreamingMode().build();
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
                        + "',  'minIdle'='1'  )";

        tEnv.executeSql(ddl);
        String sql =
                " insert into sink_redis select * from (values ('test_cluster_sink', '3', '18'))";
        TableResult tableResult = tEnv.executeSql(sql);
        tableResult.getJobClient().get().getJobExecutionResult().get();
        Preconditions.condition(clusterCommands.hget("test_cluster_sink", "3").equals("18"), "");
    }

    @Test
    public void testHgetSQL() throws Exception {
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
                        + "',   'lookup.cache.max-rows'='10', 'lookup.cache.ttl'='10', 'lookup.max-retries'='3'  )";

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

    @Test
    public void testGetSQL() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        EnvironmentSettings environmentSettings =
                EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, environmentSettings);

        clusterCommands.set("10", "1800000");
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
                        + "',   'lookup.cache.max-rows'='10', 'lookup.cache.ttl'='10', 'lookup.max-retries'='3'  )";

        String source =
                "create table source_table(username varchar, level varchar, proctime as procTime()) "
                        + "with ('connector'='datagen',  'rows-per-second'='1', "
                        + "'fields.username.kind'='sequence',  'fields.username.start'='10',  'fields.username.end'='15',"
                        + "'fields.level.kind'='sequence',  'fields.level.start'='10',  'fields.level.end'='15'"
                        + ")";

        String sink =
                "create table sink_table(username varchar, level varchar,login_time time(3)) with  ( 'connector'='redis', "
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
                        + " select concat_ws('_',s.username, s.level), s.level,  d.login_time from source_table s"
                        + "  left join dim_table for system_time as of s.proctime as d "
                        + " on d.name = s.username";
        TableResult tableResult = tEnv.executeSql(sql);
        tableResult.getJobClient().get().getJobExecutionResult().get();
        System.out.println(sql);
        Preconditions.condition(clusterCommands.hget("10_10", "10").equals("1800000"), "");
        Preconditions.condition(clusterCommands.hget("11_11", "11") == "", "");
        Preconditions.condition(clusterCommands.hget("11_11", "12") == null, "");
    }

    @Test
    public void testDel() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        clusterCommands.set("20", "20");

        Preconditions.condition(clusterCommands.get("20").equals("20"), "");
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
        Preconditions.condition(clusterCommands.get("20") == null, "");
    }

    @Test
    public void testSRem() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        clusterCommands.sadd("s", "test1", "test2");
        Preconditions.condition(clusterCommands.sismember("s", "test2"), "");
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
                tEnv.executeSql("insert into redis_sink select * from (values('s', 'test2'))");
        tableResult.getJobClient().get().getJobExecutionResult().get();
        Preconditions.condition(clusterCommands.sismember("s", "test2") == false, "");
    }

    @Test
    public void testHdel() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        clusterCommands.hset("s", "test1", "test2");
        Preconditions.condition(clusterCommands.hget("s", "test1").equals("test2"), "");
        String ddl =
                "create table redis_sink(redis_key varchar, redis_member varchar) with('connector'='redis',"
                        + "'cluster-nodes'='"
                        + CLUSTERNODES
                        + "','redis-mode'='cluster','password'='"
                        + CLUSTER_PASSWORD
                        + "','"
                        + REDIS_COMMAND
                        + "'='"
                        + RedisCommand.HDEL
                        + "') ";
        tEnv.executeSql(ddl);
        TableResult tableResult =
                tEnv.executeSql("insert into redis_sink select * from (values('s', 'test1'))");
        tableResult.getJobClient().get().getJobExecutionResult().get();
        Preconditions.condition(clusterCommands.hget("s", "test1") == null, "");
    }

    @Test
    public void testHIncryBy() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        singleRedisCommands.hset("11", "12", "1");
        Preconditions.condition(singleRedisCommands.hget("11", "12").equals("1"), "");
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
        Preconditions.condition(singleRedisCommands.hget("11", "12").equals("11"), "");
    }

    @Test
    public void testHIncryByFloat() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        singleRedisCommands.hset("11", "12", "1");
        Preconditions.condition(singleRedisCommands.hget("11", "12").equals("1"), "");
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
        String sql = " insert into sink_redis select * from (values ('11', '12', 10.1))";
        TableResult tableResult = tEnv.executeSql(sql);
        tableResult.getJobClient().get().getJobExecutionResult().get();
        System.out.println(sql);
        Preconditions.condition(singleRedisCommands.hget("11", "12").equals("11.1"), "");
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
        String s =
                new StringBuilder()
                        .append("1")
                        .append(RedisDynamicTableFactory.CACHE_SEPERATOR)
                        .append("11.3")
                        .append(RedisDynamicTableFactory.CACHE_SEPERATOR)
                        .append("10.3")
                        .toString();
        Preconditions.condition(singleRedisCommands.get("1").equals(s), "");
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
        String sql =
                " insert into sink_redis select * from (values ('1', 10.3, 10.1),('2', 10.1, 10.1),('3', 10.3, 10.1))";
        tEnv.executeSql(sql);
        System.out.println(sql);

        // create join table
        ddl =
                "create table join_table with ('command'='get', 'value.data.structure'='row') like sink_redis";
        tEnv.executeSql(ddl);
        System.out.println(ddl);

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
        System.out.println(ddl);

        // create source table
        ddl =
                "create table source_table(uid VARCHAR, username VARCHAR, proc_time as procTime()) with ('connector'='datagen', 'fields.uid.kind'='sequence', 'fields.uid.start'='1', 'fields.uid.end'='2')";
        tEnv.executeSql(ddl);
        System.out.println(ddl);

        sql =
                "insert into result_table select concat_ws('_', s.uid, s.uid), j.score from source_table as s join join_table for system_time as of s.proc_time as j  on j.uid = s.uid ";
        System.out.println(sql);
        TableResult tableResult = tEnv.executeSql(sql);
        tableResult.getJobClient().get().getJobExecutionResult().get();
        Preconditions.condition(singleRedisCommands.get("1_1").equals("10.3"), "");
        Preconditions.condition(singleRedisCommands.get("2_2").equals("10.1"), "");
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
        System.out.println(ddl);

        // create source table
        ddl =
                "create table source_table(uid VARCHAR, level varchar, username VARCHAR, proc_time as procTime()) with ('connector'='datagen', 'fields.uid.kind'='sequence', 'fields.uid.start'='10', 'fields.uid.end'='12', 'fields.level.kind'='sequence', 'fields.level.start'='10', 'fields.level.end'='12')";
        tEnv.executeSql(ddl);
        System.out.println(ddl);

        sql =
                "insert into result_table select concat_ws('_', s.uid, s.level),s.level, j.score from source_table as s join join_table for system_time as of s.proc_time as j  on j.uid = s.uid and j.level = s.level";
        System.out.println(sql);
        TableResult tableResult = tEnv.executeSql(sql);
        tableResult.getJobClient().get().getJobExecutionResult().get();
        Preconditions.condition(singleRedisCommands.hget("10_10", "10") == "", "");
        Preconditions.condition(singleRedisCommands.hget("11_11", "11").equals("10.3"), "");
        Preconditions.condition(singleRedisCommands.hget("12_12", "12") == "", "");
    }

    @Test
    public void testHgetWithUpdate() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings environmentSettings =
                EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, environmentSettings);

        clusterCommands.hset("key1", "field1", "1");
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
                        + "' )";

        String source =
                "create table source_table(age varchar, proctime as procTime()) "
                        + "with ('connector'='datagen',  'rows-per-second'='1', "
                        + "'fields.age.kind'='sequence',  'fields.age.start'='1',  'fields.age.end'='99'"
                        + ")";

        String sink =
                "create table sink_table(username varchar, level varchar,age varchar) with ( 'connector'='print')";

        tEnv.executeSql(source);
        tEnv.executeSql(dim);
        tEnv.executeSql(sink);

        //        tEnv.executeSql("insert into dim_table select 'key1', 'field1', age from
        // source_table ");
        String sql =
                " insert into sink_table "
                        + " select d.name,  d.level, d.age from source_table s"
                        + "  left join dim_table for system_time as of s.proctime as d "
                        + " on d.name ='key1' and d.level ='field1'";
        TableResult tableResult = tEnv.executeSql(sql);
        tableResult.getJobClient().get().getJobExecutionResult().get();
        System.out.println(sql);
    }

    @Test
    public void testSinkValueWithExpire() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        LocalTime localTime = LocalTime.now();
        int wait = 100;
        localTime = localTime.plusSeconds(wait);
        String dim =
                "create table sink_redis(name varchar, level varchar, age varchar) with ( 'connector'='redis', "
                        + "'cluster-nodes'='"
                        + CLUSTERNODES
                        + "','redis-mode'='cluster', 'ttl'='10','expire.on.time'='"
                        + localTime.toString()
                        + "', 'password'='"
                        + CLUSTER_PASSWORD
                        + "','"
                        + REDIS_COMMAND
                        + "'='"
                        + RedisCommand.HSET
                        + "' )";

        tEnv.executeSql(dim);
        String sql = " insert into sink_redis select * from (values ('1', '11.3', '10.3'))";
        TableResult tableResult = tEnv.executeSql(sql);
        tableResult.getJobClient().get().getJobExecutionResult().get();
        System.out.println(sql);
        Preconditions.condition(clusterCommands.exists("1") == 1, "");
        Thread.sleep(wait * 1000);
        Preconditions.condition(clusterCommands.exists("1") == 0, "");
    }
}
