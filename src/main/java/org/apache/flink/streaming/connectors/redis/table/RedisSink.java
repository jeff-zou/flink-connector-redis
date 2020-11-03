/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.redis.table;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisClusterConfig;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisConfigBase;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisSentinelConfig;
import org.apache.flink.streaming.connectors.redis.common.container.RedisCommandsContainer;
import org.apache.flink.streaming.connectors.redis.common.container.RedisCommandsContainerBuilder;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisDataType;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

import org.apache.flink.streaming.connectors.redis.common.mapper.row.RowRedisMapper;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.constraints.UniqueConstraint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * A sink that delivers data to a Redis channel using the Jedis client.
 * <p> The sink takes two arguments {@link FlinkJedisConfigBase} and {@link RedisMapper}.
 * <p> When {@link FlinkJedisPoolConfig} is passed as the first argument,
 * the sink will create connection using {@link redis.clients.jedis.JedisPool}. Please use this when
 * you want to connect to a single Redis server.
 * <p> When {@link FlinkJedisSentinelConfig} is passed as the first argument, the sink will create connection
 * using {@link redis.clients.jedis.JedisSentinelPool}. Please use this when you want to connect to Sentinel.
 * <p> Please use {@link FlinkJedisClusterConfig} as the first argument if you want to connect to
 * a Redis Cluster.
 *
 * <p>Example:
 *
 * <pre>
 *{@code
 *public static class RedisExampleMapper implements RedisMapper<Tuple2<String, String>> {
 *
 *    private RedisCommand redisCommand;
 *
 *    public RedisExampleMapper(RedisCommand redisCommand){
 *        this.redisCommand = redisCommand;
 *    }
 *    public RedisCommandDescription getCommandDescription() {
 *        return new RedisCommandDescription(redisCommand, REDIS_ADDITIONAL_KEY);
 *    }
 *    public String getKeyFromData(Tuple2<String, String> data) {
 *        return data.f0;
 *    }
 *    public String getValueFromData(Tuple2<String, String> data) {
 *        return data.f1;
 *    }
 *}
 *JedisPoolConfig jedisPoolConfig = new JedisPoolConfig.Builder()
 *    .setHost(REDIS_HOST).setPort(REDIS_PORT).build();
 *new RedisSink<String>(jedisPoolConfig, new RedisExampleMapper(RedisCommand.LPUSH));
 *}</pre>
 *
 * @param <IN> Type of the elements emitted by this sink
 */
public class RedisSink<IN> extends RichSinkFunction<IN> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(RedisSink.class);

    /**
     * This additional key needed for {@link RedisDataType#HASH} and {@link RedisDataType#SORTED_SET}.
     * Other {@link RedisDataType} works only with two variable i.e. name of the list and value to be added.
     * But for {@link RedisDataType#HASH} and {@link RedisDataType#SORTED_SET} we need three variables.
     * <p>For {@link RedisDataType#HASH} we need hash name, hash key and element.
     * {@code additionalKey} used as hash name for {@link RedisDataType#HASH}
     * <p>For {@link RedisDataType#SORTED_SET} we need set name, the element and it's score.
     * {@code additionalKey} used as set name for {@link RedisDataType#SORTED_SET}
     */
    private String additionalKey;

    /**
     * This additional time to live is optional for {@link RedisDataType#HASH} and required for {@link RedisCommand#SETEX}.
     * It sets the TTL for a specific key.
     */
    private Integer additionalTTL;

    private RedisMapper<IN> redisSinkMapper;
    private RedisCommand redisCommand;

    private FlinkJedisConfigBase flinkJedisConfigBase;
    private RedisCommandsContainer redisCommandsContainer;

    private List<Integer> valueIndexs;

    private List<Integer> keyIndexs;

    private String partitionColumn;

    private Integer partitionIndex;

    /**
     * Creates a new {@link RedisSink} that connects to the Redis server.
     *
     * @param flinkJedisConfigBase The configuration of {@link FlinkJedisConfigBase}
     * @param redisSinkMapper This is used to generate Redis command and key value from incoming elements.
     */
    public RedisSink(FlinkJedisConfigBase flinkJedisConfigBase, RedisMapper<IN> redisSinkMapper, TableSchema tableSchema) {
        Objects.requireNonNull(flinkJedisConfigBase, "Redis connection pool config should not be null");
        Objects.requireNonNull(redisSinkMapper, "Redis Mapper can not be null");
        Objects.requireNonNull(redisSinkMapper.getCommandDescription(), "Redis Mapper data type description can not be null");

        this.flinkJedisConfigBase = flinkJedisConfigBase;

        this.redisSinkMapper = redisSinkMapper;
        RedisCommandDescription redisCommandDescription = redisSinkMapper.getCommandDescription();

        this.redisCommand = redisCommandDescription.getCommand();
        this.additionalTTL = redisCommandDescription.getAdditionalTTL();
        this.additionalKey = redisCommandDescription.getAdditionalKey();
        this.partitionColumn = redisCommandDescription.getPartitionColumn();
        getKeyValueIndex(tableSchema);
    }

    private void getKeyValueIndex(TableSchema tableSchema) {
        this.keyIndexs = new ArrayList<>();
        this.valueIndexs = new ArrayList<>();
        if(tableSchema == null ){
            this.keyIndexs.add(0);
            return;
        }
        String[] fieldNames = tableSchema.getFieldNames();
        getPartitionIndex(fieldNames);

        getKeyIndex(tableSchema, fieldNames);
    }

    private void getPartitionIndex(String[] fieldNames) {
        if(StringUtils.isEmpty(partitionColumn))
            return;

        for(int i=0;i<fieldNames.length;i++){
            if(fieldNames[i].equals(partitionColumn)){
                this.partitionIndex = i;
                break;
            }

        }
    }

    private void getKeyIndex( TableSchema tableSchema,  String[] fieldNames) {

        Optional<UniqueConstraint> uniqueConstraint = tableSchema.getPrimaryKey();
        if(!uniqueConstraint.isPresent()){
            this.keyIndexs.add(0);
            return;
        }

        List<String> primayKeyList = uniqueConstraint.get().getColumns();
        for(int i=0;i<fieldNames.length;i++){
            boolean flag = false;
            for(String primaryKey : primayKeyList){
                if(primaryKey.equals(fieldNames[i])){
                    this.keyIndexs.add(i);
                    flag = true;
                    break;
                }

            }

            if(!flag)
                this.valueIndexs.add(i);
        }
    }

    /**
     * Called when new data arrives to the sink, and forwards it to Redis channel.
     * Depending on the specified Redis data type (see {@link RedisDataType}),
     * a different Redis command will be applied.
     * Available commands are RPUSH, LPUSH, SADD, PUBLISH, SET, SETEX, PFADD, HSET, ZADD.
     *
     * @param input The incoming data
     */
    @Override
    public void invoke(IN input, Context context) throws Exception {
        String key = redisSinkMapper.getKeyFromData(input, keyIndexs);
        String value = redisSinkMapper.getValueFromData(input, valueIndexs);

        Optional<String> optAdditionalKey = redisSinkMapper.getAdditionalKey(input);
        Optional<Integer> optAdditionalTTL = redisSinkMapper.getAdditionalTTL(input);
        String additionalFinalKey = optAdditionalKey.orElse(this.additionalKey);

        if(partitionIndex != null){
            String partitionValue = redisSinkMapper.getPartitionFromData(input, partitionIndex);
            if(redisCommand == RedisCommand.ZADD || redisCommand == RedisCommand.ZINCRBY || redisCommand == RedisCommand.ZREM || redisCommand == RedisCommand.HSET || redisCommand == RedisCommand.HINCRBY ){
                StringBuilder stringBuilder = new StringBuilder(partitionValue).append(RowRedisMapper.REDIS_VALUE_SEPERATOR).append(additionalFinalKey);
                additionalFinalKey = stringBuilder.toString();
            } else {
                StringBuilder stringBuilder = new StringBuilder(partitionValue).append(RowRedisMapper.REDIS_VALUE_SEPERATOR).append(key);
                key = stringBuilder.toString();
            }
        }

        switch (redisCommand) {
            case RPUSH:
                this.redisCommandsContainer.rpush(key, value);
                break;
            case LPUSH:
                this.redisCommandsContainer.lpush(key, value);
                break;
            case SADD:
                this.redisCommandsContainer.sadd(key, value);
                break;
            case SET:
                this.redisCommandsContainer.set(key, value);
                break;
            case SETEX:
                this.redisCommandsContainer.setex(key, value, optAdditionalTTL.orElse(this.additionalTTL));
                break;
            case PFADD:
                this.redisCommandsContainer.pfadd(key, value);
                break;
            case PUBLISH:
                this.redisCommandsContainer.publish(key, value);
                break;
            case ZADD:
                this.redisCommandsContainer.zadd(additionalFinalKey, value, key);
                break;
            case ZINCRBY:
                this.redisCommandsContainer.zincrBy(additionalFinalKey, value, key);
                break;
            case ZREM:
                this.redisCommandsContainer.zrem(additionalFinalKey, key);
                break;
            case HSET:
                this.redisCommandsContainer.hset(additionalFinalKey, key, value,
                        optAdditionalTTL.orElse(this.additionalTTL));
                break;
            case HINCRBY:
                this.redisCommandsContainer.hincrBy(additionalFinalKey, key, Long.valueOf(value), optAdditionalTTL.orElse(this.additionalTTL));
                break;
            case INCRBY:
                this.redisCommandsContainer.incrBy(key, Long.valueOf(value));
                break;
            case INCRBY_EX:
                this.redisCommandsContainer.incrByEx(key, Long.valueOf(value), optAdditionalTTL.orElse(this.additionalTTL));
                break;
            case DECRBY:
                this.redisCommandsContainer.decrBy(key, Long.valueOf(value));
                break;
            case DESCRBY_EX:
                this.redisCommandsContainer.decrByEx(key, Long.valueOf(value), optAdditionalTTL.orElse(this.additionalTTL));
                break;
            default:
                throw new IllegalArgumentException("Cannot process such data type: " + redisCommand);
        }
    }

    /**
     * Initializes the connection to Redis by either cluster or sentinels or single server.
     *
     * @throws IllegalArgumentException if jedisPoolConfig, jedisClusterConfig and jedisSentinelConfig are all null
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        try {
            this.redisCommandsContainer = RedisCommandsContainerBuilder.build(this.flinkJedisConfigBase);
            this.redisCommandsContainer.open();
        } catch (Exception e) {
            LOG.error("Redis has not been properly initialized: ", e);
            throw e;
        }
    }

    /**
     * Closes commands container.
     * @throws IOException if command container is unable to close.
     */
    @Override
    public void close() throws IOException {
        if (redisCommandsContainer != null) {
            redisCommandsContainer.close();
        }
    }
}
