package org.apache.flink.streaming.connectors.redis.table;

import static org.apache.flink.streaming.connectors.redis.common.config.RedisValidator.REDIS_COMMAND;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.table.base.SQLWithUtil;
import org.apache.flink.streaming.connectors.redis.table.base.TestRedisConfigBase;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.jupiter.api.Test;

public class FlinkCDCTest extends TestRedisConfigBase {

    @Test
    public void testCdc() throws Exception {
        String ddl =
                "CREATE TABLE orders (\n"
                        + "     order_id INT,\n"
                        + "     customer_name STRING,\n"
                        + "     price DECIMAL(10, 5),\n"
                        + "     product_id INT,\n"
                        + "     PRIMARY KEY(order_id) NOT ENFORCED\n"
                        + "     ) WITH (\n"
                        + "     'connector' = 'mysql-cdc',\n"
                        + "     'hostname' = '10.11.69.176',\n"
                        + "     'port' = '3306',\n"
                        + "     'username' = 'test',\n"
                        + "     'password' = '123456',\n"
                        + "     'database-name' = 'cdc',\n"
                        + "     'table-name' = 'orders');";
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        env.setParallelism(1);
        tEnv.executeSql(ddl);

        String sink =
                "create table sink_redis(name varchar, level varchar, age varchar) with (  "
                        + SQLWithUtil.sigleWith()
                        + " '"
                        + REDIS_COMMAND
                        + "'='"
                        + RedisCommand.HSET
                        + "' )";
        tEnv.executeSql(sink);
        TableResult tableResult =
                tEnv.executeSql(
                        "insert into sink_redis select cast(order_id as string), customer_name,  cast(product_id as string) from orders /*+ OPTIONS('server-id'='5401-5404') */");
        tableResult.getJobClient().get().getJobExecutionResult().get();
    }
}
