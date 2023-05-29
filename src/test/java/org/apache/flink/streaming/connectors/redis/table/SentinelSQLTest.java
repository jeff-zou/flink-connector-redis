package org.apache.flink.streaming.connectors.redis.table;

import static org.apache.flink.streaming.connectors.redis.descriptor.RedisValidator.REDIS_COMMAND;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.jupiter.api.Test;

/** Created by jeff.zou on 2020/9/10. */
public class SentinelSQLTest {

    @Test
    public void testNoPrimaryKeyInsertSQL() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings environmentSettings =
                EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, environmentSettings);

        String ddl =
                "create table sink_redis(username VARCHAR, passport time(3)) with ( 'connector'='redis', "
                        + "'master.name'='mymaster','sentinels.info'='10.11.96.185:16379,10.11.96.186:16379,10.11.96.187:16379', 'redis-mode'='sentinel'"
                        + ",'password'='********','"
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
