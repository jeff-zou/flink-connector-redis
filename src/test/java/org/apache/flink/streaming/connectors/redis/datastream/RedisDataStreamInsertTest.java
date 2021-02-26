package org.apache.flink.streaming.connectors.redis.datastream;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisClusterConfig;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisConfigBase;
import org.apache.flink.streaming.connectors.redis.common.config.RedisOptions;
import org.apache.flink.streaming.connectors.redis.common.hanlder.RedisHandlerServices;
import org.apache.flink.streaming.connectors.redis.common.hanlder.RedisMapperHandler;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.streaming.connectors.redis.table.RedisSink;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.GenericRowData;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static org.apache.flink.streaming.connectors.redis.descriptor.RedisValidator.*;

/**
 * Created by jeff.zou on 2021/2/26.
 */
public class RedisDataStreamInsertTest {


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
        FlinkJedisClusterConfig config = new FlinkJedisClusterConfig.Builder().setNodes(set).setPassword("*****")
                .build();
        return config;
    }

    @Test
    public void testDateStreamInsert() throws  Exception {
        Map properties = new HashMap();
        properties.put(RedisOptions.ADDITIONALKEY.key(), "province");
        properties.put(REDIS_PARTITION_COLUMN, "school");
        properties.put(REDIS_MODE, REDIS_CLUSTER);
        properties.put(REDIS_COMMAND, RedisCommand.HSET.name());

        RedisMapper redisMapper = RedisHandlerServices
                .findRedisHandler(RedisMapperHandler.class, properties)
                .createRedisMapper(properties);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        GenericRowData genericRowData = new GenericRowData(5);
        genericRowData.setField(0, "tom");
        genericRowData.setField(1, "math");
        genericRowData.setField(2, "150");
        genericRowData.setField(3, "guangdong");
        genericRowData.setField(4, "yizhong");
        DataStream<GenericRowData> dataStream = env.fromElements(genericRowData);

        TableSchema tableSchema =  new TableSchema.Builder().field("name", DataTypes.STRING().notNull()).field("subject", DataTypes.STRING()).field("scope", DataTypes.INT())
                .field("province", DataTypes.STRING())
                .field("school", DataTypes.STRING()).primaryKey("name").build();

        FlinkJedisConfigBase conf = getLocalRedisClusterConfig();
        RedisSink redisSink = new RedisSink<>(conf, redisMapper, null);

        dataStream.addSink(redisSink);
        env.execute("RedisSinkTest");
    }
}