package org.apache.flink.streaming.connectors.redis;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.cluster.RedisClusterClient;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @Author: Jeff Zou @Date: 2022/10/14 10:07
 */
public class TestRedisConfigBase {

    private static final Logger LOG = LoggerFactory.getLogger(TestRedisConfigBase.class);

    public static final String REDIS_HOST = "10.11.69.176";
    public static final int REDIS_PORT = 6379;
    public static final String REDIS_PASSWORD = "***";

    public static final String CLUSTER_PASSWORD = "***";
    public static final String CLUSTERNODES =
            "10.11.80.147:7000,10.11.80.147:7001,10.11.80.147:8000,10.11.80.147:8001,10.11.80.147:9000,10.11.80.147:9001";

    @BeforeEach
    protected void cleanSingle() {
        RedisURI redisURI =
                RedisURI.builder()
                        .withHost(REDIS_HOST)
                        .withPort(REDIS_PORT)
                        .withPassword(REDIS_PASSWORD.toCharArray())
                        .build();
        RedisClient redisClient = RedisClient.create(redisURI);
        redisClient.connect().sync().flushdb();
        redisClient.shutdown();
        LOG.info("clear data in redis: {}", REDIS_HOST);
    }

    @BeforeEach
    protected void cleanCluster() {
        List<RedisURI> redisURIS =
                Arrays.stream(CLUSTERNODES.split(","))
                        .map(
                                node -> {
                                    String[] redis = node.split(":");
                                    return RedisURI.builder()
                                            .withPassword(CLUSTER_PASSWORD.toCharArray())
                                            .withHost(redis[0])
                                            .withPort(Integer.parseInt(redis[1]))
                                            .build();
                                })
                        .collect(Collectors.toList());

        RedisClusterClient clusterClient = RedisClusterClient.create(redisURIS);
        clusterClient.connect().sync().flushdb();
        clusterClient.shutdown();
        LOG.info("clear data in redis: {}", CLUSTERNODES);
    }
}
