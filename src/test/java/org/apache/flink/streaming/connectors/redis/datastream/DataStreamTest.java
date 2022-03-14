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
import org.apache.flink.table.data.GenericRowData;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.embedded.RedisServer;
import redis.embedded.RedisServerBuilder;

import static org.apache.flink.streaming.connectors.redis.descriptor.RedisValidator.REDIS_COMMAND;
import static org.apache.flink.streaming.connectors.redis.descriptor.RedisValidator.REDIS_MODE;
import static org.apache.flink.streaming.connectors.redis.descriptor.RedisValidator.REDIS_SINGLE;

/** Created by jeff.zou on 2021/2/26. */
public class DataStreamTest {

    private RedisServer redisServer;

    @Before
    public void before() {
       redisServer = RedisServer.builder().setting("maxheap 512000").build();
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

        GenericRowData genericRowData = new GenericRowData(3);
        genericRowData.setField(0, "tom");
        genericRowData.setField(1, "math");
        genericRowData.setField(2, "152");
        DataStream<GenericRowData> dataStream = env.fromElements(genericRowData, genericRowData);

        RedisCacheOptions redisCacheOptions =
                new RedisCacheOptions.Builder().setCacheMaxSize(100).setCacheTTL(10L).build();
        FlinkJedisConfigBase conf = new FlinkJedisPoolConfig.Builder().setHost("localhost").setPort(6379).build();
        RedisSinkFunction redisSinkFunction =
                new RedisSinkFunction<>(conf, redisMapper, redisCacheOptions);

        dataStream.addSink(redisSinkFunction).setParallelism(1);
        env.execute("RedisSinkTest");
    }

    @After
    public void stopRedis() {
        redisServer.stop();
    }
}
