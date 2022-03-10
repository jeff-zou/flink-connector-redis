package org.apache.flink.streaming.connectors.redis.datastream;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisClusterConfig;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisConfigBase;
import org.apache.flink.streaming.connectors.redis.common.config.RedisLookupOptions;
import org.apache.flink.streaming.connectors.redis.common.config.RedisOptions;
import org.apache.flink.streaming.connectors.redis.common.hanlder.RedisHandlerServices;
import org.apache.flink.streaming.connectors.redis.common.hanlder.RedisMapperHandler;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisSinkMapper;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.streaming.connectors.redis.table.RedisSinkFunction;
import org.apache.flink.streaming.connectors.redis.table.RedisLookupFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.types.DataType;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.HashSet;

import static org.apache.flink.streaming.connectors.redis.descriptor.RedisValidator.*;
import static org.apache.flink.streaming.connectors.redis.table.SQLTest.PASSWORD;

/**
 * Created by jeff.zou on 2021/2/26.
 */
public class DataStreamTest {


    public static FlinkJedisClusterConfig getLocalRedisClusterConfig(){
        InetSocketAddress host0 = new InetSocketAddress("10.11.80.147", 7000);
        InetSocketAddress host1 = new InetSocketAddress("10.11.80.147", 7001);
        InetSocketAddress host2 = new InetSocketAddress("10.11.80.147", 8000);
        InetSocketAddress host3 = new InetSocketAddress("10.11.80.147", 8001);
        InetSocketAddress host4 = new InetSocketAddress("10.11.80.147", 9000);
        InetSocketAddress host5 = new InetSocketAddress("10.11.80.147", 9001);


        HashSet<InetSocketAddress> set = new HashSet<>();
        set.add(host0);
        set.add(host1);
        set.add(host2);
        set.add(host3);
        set.add(host4);
        set.add(host5);
        FlinkJedisClusterConfig config = new FlinkJedisClusterConfig.Builder().setNodes(set).setPassword(PASSWORD)
                .build();
        return config;
    }

    /*
       hget tom math
       return: 150
        */
    @Test
    public void testDateStreamInsert() throws  Exception {

        Configuration configuration = new Configuration();
        configuration.setString(REDIS_MODE, REDIS_CLUSTER);
        configuration.setString(REDIS_COMMAND, RedisCommand.HSET.name());

        RedisSinkMapper redisMapper = (RedisSinkMapper)RedisHandlerServices
                .findRedisHandler(RedisMapperHandler.class, configuration.toMap())
                .createRedisMapper(configuration);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        GenericRowData genericRowData = new GenericRowData(3);
        genericRowData.setField(0, "tom");
        genericRowData.setField(1, "math");
        genericRowData.setField(2, "151");
        DataStream<GenericRowData> dataStream = env.fromElements(genericRowData);

       ResolvedSchema resolvedSchema = ResolvedSchema.physical(new String[]{"name", "subject", "score"}, new DataType[]{DataTypes.STRING().notNull(), DataTypes.STRING().notNull(), DataTypes.INT().notNull()});
        FlinkJedisConfigBase conf = getLocalRedisClusterConfig();
        RedisSinkFunction redisSinkFunction = new RedisSinkFunction<>(conf, redisMapper, resolvedSchema);

        dataStream.addSink(redisSinkFunction);
        env.execute("RedisSinkTest");
    }



}