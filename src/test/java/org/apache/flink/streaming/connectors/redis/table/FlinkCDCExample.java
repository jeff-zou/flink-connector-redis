/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.redis.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.command.RedisCommand;
import org.apache.flink.streaming.connectors.redis.table.base.TestRedisConfigBase;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.streaming.connectors.redis.config.RedisValidator.REDIS_COMMAND;

public class FlinkCDCExample extends TestRedisConfigBase {

    public static void main(String[] args) throws Exception {
        cdcExample();
    }

    public static void cdcExample() throws Exception {
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
                        + "'connector'='redis', "
                        + "'host'='"
                        + REDIS_HOST
                        + "','port'='"
                        + REDIS_PORT
                        + "', 'redis-mode'='single','password'='"
                        + REDIS_PASSWORD
                        + "',"
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
