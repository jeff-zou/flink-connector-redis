package org.apache.flink.streaming.connectors.redis.common.container;

import org.apache.commons.compress.utils.Sets;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisClusterConfig;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisConfigBase;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisSentinelConfig;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisSentinelPool;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/** The builder for {@link RedisCommandsContainer}. */
public class RedisCommandsContainerBuilder {

    /**
     * Initialize the {@link RedisCommandsContainer} based on the instance type.
     *
     * @param flinkJedisConfigBase configuration base
     * @return @throws IllegalArgumentException if jedisPoolConfig, jedisClusterConfig and
     *     jedisSentinelConfig are all null
     */
    public static RedisCommandsContainer build(FlinkJedisConfigBase flinkJedisConfigBase) {
        if (flinkJedisConfigBase instanceof FlinkJedisPoolConfig) {
            FlinkJedisPoolConfig flinkJedisPoolConfig = (FlinkJedisPoolConfig) flinkJedisConfigBase;
            return RedisCommandsContainerBuilder.build(flinkJedisPoolConfig);
        } else if (flinkJedisConfigBase instanceof FlinkJedisClusterConfig) {
            FlinkJedisClusterConfig flinkJedisClusterConfig =
                    (FlinkJedisClusterConfig) flinkJedisConfigBase;
            return RedisCommandsContainerBuilder.build(flinkJedisClusterConfig);
        } else if (flinkJedisConfigBase instanceof FlinkJedisSentinelConfig) {
            FlinkJedisSentinelConfig flinkJedisSentinelConfig =
                    (FlinkJedisSentinelConfig) flinkJedisConfigBase;
            return RedisCommandsContainerBuilder.build(flinkJedisSentinelConfig);
        } else {
            throw new IllegalArgumentException("Jedis configuration not found");
        }
    }

    /**
     * Builds container for single Redis environment.
     *
     * @param jedisPoolConfig configuration for JedisPool
     * @return container for single Redis environment
     * @throws NullPointerException if jedisPoolConfig is null
     */
    public static RedisCommandsContainer build(FlinkJedisPoolConfig jedisPoolConfig) {
        Objects.requireNonNull(jedisPoolConfig, "Redis pool config should not be Null");

        GenericObjectPoolConfig genericObjectPoolConfig = new GenericObjectPoolConfig();
        genericObjectPoolConfig.setMaxIdle(jedisPoolConfig.getMaxIdle());
        genericObjectPoolConfig.setMaxTotal(jedisPoolConfig.getMaxTotal());
        genericObjectPoolConfig.setMinIdle(jedisPoolConfig.getMinIdle());

        JedisPool jedisPool =
                new JedisPool(
                        genericObjectPoolConfig,
                        jedisPoolConfig.getHost(),
                        jedisPoolConfig.getPort(),
                        jedisPoolConfig.getConnectionTimeout(),
                        jedisPoolConfig.getPassword(),
                        jedisPoolConfig.getDatabase());
        return new RedisContainer(jedisPool);
    }

    /**
     * Builds container for Redis Cluster environment.
     *
     * @param jedisClusterConfig configuration for JedisCluster
     * @return container for Redis Cluster environment
     * @throws NullPointerException if jedisClusterConfig is null
     */
    public static RedisCommandsContainer build(FlinkJedisClusterConfig jedisClusterConfig) {
        Objects.requireNonNull(jedisClusterConfig, "Redis cluster config should not be Null");

        GenericObjectPoolConfig genericObjectPoolConfig = new GenericObjectPoolConfig();
        genericObjectPoolConfig.setMaxIdle(jedisClusterConfig.getMaxIdle());
        genericObjectPoolConfig.setMaxTotal(jedisClusterConfig.getMaxTotal());
        genericObjectPoolConfig.setMinIdle(jedisClusterConfig.getMinIdle());

        JedisCluster jedisCluster =
                new JedisCluster(
                        jedisClusterConfig.getNodes(),
                        jedisClusterConfig.getConnectionTimeout(),
                        jedisClusterConfig.getConnectionTimeout(),
                        jedisClusterConfig.getMaxRedirections(),
                        jedisClusterConfig.getPassword(),
                        genericObjectPoolConfig);
        return new RedisClusterContainer(jedisCluster);
    }

    /**
     * Builds container for Redis Sentinel environment.
     *
     * @param jedisSentinelConfig configuration for JedisSentinel
     * @return container for Redis sentinel environment
     * @throws NullPointerException if jedisSentinelConfig is null
     */
    public static RedisCommandsContainer build(FlinkJedisSentinelConfig jedisSentinelConfig) {
        Objects.requireNonNull(jedisSentinelConfig, "Redis sentinel config should not be Null");

        GenericObjectPoolConfig genericObjectPoolConfig = new GenericObjectPoolConfig();
        genericObjectPoolConfig.setMaxIdle(jedisSentinelConfig.getMaxIdle());
        genericObjectPoolConfig.setMaxTotal(jedisSentinelConfig.getMaxTotal());
        genericObjectPoolConfig.setMinIdle(jedisSentinelConfig.getMinIdle());

        String clientName = "flink_sql_redis_connector";
        HashSet<String> set = new HashSet<>();
        Collections.addAll(set, jedisSentinelConfig.getSentinelsInfo().split(","));
        JedisSentinelPool jedisSentinelPool =
                new JedisSentinelPool(
                        jedisSentinelConfig.getMasterName(),
                        set,
                        genericObjectPoolConfig,
                        jedisSentinelConfig.getConnectionTimeout(),
                        jedisSentinelConfig.getSoTimeout(),
                        jedisSentinelConfig.getPassword(),
                        jedisSentinelConfig.getDatabase(), clientName,
                        jedisSentinelConfig.getConnectionTimeout(),
                        jedisSentinelConfig.getSoTimeout(),jedisSentinelConfig.getSentinelsPassword(), clientName);

        return new RedisContainer(jedisSentinelPool);
    }
}
