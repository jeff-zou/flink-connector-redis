package org.apache.flink.streaming.connectors.redis.container;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisClient;
import io.lettuce.core.cluster.RedisClusterClient;

import org.apache.flink.streaming.connectors.redis.config.FlinkConfigBase;

/** The builder for {@link RedisCommandsContainer}. */
public class RedisCommandsContainerBuilder {

    /**
     * @param flinkConfigBase
     * @return
     */
    public static RedisCommandsContainer build(FlinkConfigBase flinkConfigBase) {
        AbstractRedisClient redisClient = RedisClientBuilder.build(flinkConfigBase);
        if (redisClient instanceof RedisClusterClient) {
            return new RedisClusterContainer((RedisClusterClient) redisClient);
        } else {
            return new RedisContainer((RedisClient) redisClient);
        }
    }
}
