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

package org.apache.flink.streaming.connectors.redis.datastream;

import static org.apache.flink.streaming.connectors.redis.config.RedisOptions.TTL;
import static org.apache.flink.streaming.connectors.redis.config.RedisValidator.REDIS_COMMAND;
import static org.apache.flink.streaming.connectors.redis.config.RedisValidator.REDIS_MODE;
import static org.apache.flink.streaming.connectors.redis.config.RedisValidator.REDIS_SINGLE;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.command.RedisCommand;
import org.apache.flink.streaming.connectors.redis.config.FlinkConfigBase;
import org.apache.flink.streaming.connectors.redis.config.FlinkSingleConfig;
import org.apache.flink.streaming.connectors.redis.mapper.RedisSinkMapper;
import org.apache.flink.streaming.connectors.redis.mapper.RowRedisSinkMapper;
import org.apache.flink.streaming.connectors.redis.stream.RedisSink;
import org.apache.flink.streaming.connectors.redis.table.base.TestRedisConfigBase;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.writer.BinaryRowWriter;
import org.apache.flink.table.types.DataType;
import org.junit.jupiter.api.Test;
import org.junit.platform.commons.util.Preconditions;

import java.util.Arrays;
import java.util.List;

/** Created by jeff.zou on 2021/2/26. */
public class DataStreamTest extends TestRedisConfigBase {

    @Test
    public void testDateStreamInsert() throws Exception {

        singleRedisCommands.del("tom");
        Configuration configuration = new Configuration();
        configuration.setString(REDIS_MODE, REDIS_SINGLE);
        configuration.setString(REDIS_COMMAND, RedisCommand.HSET.name());
        configuration.setInteger(TTL, 10);

        RedisSinkMapper redisMapper = new RowRedisSinkMapper(RedisCommand.HSET, configuration);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        BinaryRowData binaryRowData = new BinaryRowData(3);
        BinaryRowWriter binaryRowWriter = new BinaryRowWriter(binaryRowData);
        binaryRowWriter.writeString(0, StringData.fromString("tom"));
        binaryRowWriter.writeString(1, StringData.fromString("math"));
        binaryRowWriter.writeString(2, StringData.fromString("152"));

        DataStream<BinaryRowData> dataStream = env.fromElements(binaryRowData, binaryRowData);

        List<String> columnNames = Arrays.asList("name", "subject", "scope");
        List<DataType> columnDataTypes =
                Arrays.asList(DataTypes.STRING(), DataTypes.STRING(), DataTypes.STRING());
        ResolvedSchema resolvedSchema = ResolvedSchema.physical(columnNames, columnDataTypes);

        FlinkConfigBase conf =
                new FlinkSingleConfig.Builder()
                        .setHost(REDIS_HOST)
                        .setPort(REDIS_PORT)
                        .setPassword(REDIS_PASSWORD)
                        .build();

        RedisSink redisSink = new RedisSink<>(conf, redisMapper, resolvedSchema, configuration);

        dataStream.sinkTo(redisSink).setParallelism(1);
        env.execute("RedisSinkTest");

        Preconditions.condition(singleRedisCommands.hget("tom", "math").equals("152"), "");
    }
}
