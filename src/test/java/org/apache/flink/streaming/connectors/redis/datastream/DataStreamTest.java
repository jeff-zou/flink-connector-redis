package org.apache.flink.streaming.connectors.redis.datastream;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisConfigBase;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.config.RedisCacheOptions;
import org.apache.flink.streaming.connectors.redis.common.hanlder.RedisHandlerServices;
import org.apache.flink.streaming.connectors.redis.common.hanlder.RedisMapperHandler;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisSinkMapper;
import org.apache.flink.streaming.connectors.redis.table.RedisSinkFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.writer.BinaryRowWriter;
import org.apache.flink.table.types.DataType;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.embedded.RedisServer;

import java.util.Arrays;
import java.util.List;

import static org.apache.flink.streaming.connectors.redis.descriptor.RedisValidator.REDIS_COMMAND;
import static org.apache.flink.streaming.connectors.redis.descriptor.RedisValidator.REDIS_MODE;
import static org.apache.flink.streaming.connectors.redis.descriptor.RedisValidator.REDIS_SINGLE;
import static org.apache.flink.streaming.connectors.redis.table.SQLTest.PASSWORD;

/** Created by jeff.zou on 2021/2/26. */
public class DataStreamTest {

    private RedisServer redisServer;

    @Before
    public void before() throws Exception {
       redisServer = RedisServer.builder().port(6379).setting("maxheap 51200").build();
       redisServer.start();
    }

    /*
    hget tom math
    return: 150
     */
    @Test
    public void testDateStreamInsert() throws Exception {

        Configuration configuration = new Configuration();
        configuration.setString(REDIS_MODE, REDIS_SINGLE);
        configuration.setString(REDIS_COMMAND, RedisCommand.HSET.name());

        RedisSinkMapper redisMapper =
                (RedisSinkMapper)
                        RedisHandlerServices.findRedisHandler(
                                        RedisMapperHandler.class, configuration.toMap())
                                .createRedisMapper(configuration);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        BinaryRowData binaryRowData = new BinaryRowData(3);
        BinaryRowWriter binaryRowWriter = new BinaryRowWriter(binaryRowData);
        binaryRowWriter.writeString(0, StringData.fromString("tom"));
        binaryRowWriter.writeString(1, StringData.fromString("math"));
        binaryRowWriter.writeString(2, StringData.fromString("152"));

        DataStream<BinaryRowData> dataStream = env.fromElements(binaryRowData, binaryRowData);

        List<String> columnNames = Arrays.asList("name","subject", "scope");
        List<DataType> columnDataTypes = Arrays.asList(DataTypes.STRING(), DataTypes.STRING(), DataTypes.STRING());
        ResolvedSchema resolvedSchema = ResolvedSchema.physical(columnNames, columnDataTypes);

        RedisCacheOptions redisCacheOptions =
                new RedisCacheOptions.Builder().setCacheMaxSize(100).setCacheTTL(10L).build();
        FlinkJedisConfigBase conf = new FlinkJedisPoolConfig.Builder().setHost("10.11.80.147").setPort(7001).setPassword(PASSWORD).build();

        RedisSinkFunction redisSinkFunction =
                new RedisSinkFunction<>(conf, redisMapper, redisCacheOptions, resolvedSchema);

        dataStream.addSink(redisSinkFunction).setParallelism(1);
        env.execute("RedisSinkTest");
    }

    @After
    public void stopRedis() {
        redisServer.stop();
    }
}
