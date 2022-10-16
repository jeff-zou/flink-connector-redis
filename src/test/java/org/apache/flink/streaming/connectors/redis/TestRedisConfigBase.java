package org.apache.flink.streaming.connectors.redis;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.sync.RedisClusterCommands;
import org.junit.jupiter.api.AfterEach;
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
    private RedisClient redisClient;
    protected StatefulRedisConnection<String, String> singleConnect;
    protected RedisCommands singleRedisCommands;

    private RedisClusterClient clusterClient;
    protected StatefulRedisClusterConnection<String, String> clusterConnection;
    protected RedisClusterCommands clusterCommands;

    @BeforeEach
    public void cleanSingle() {
        RedisURI redisURI =
                RedisURI.builder()
                        .withHost(REDIS_HOST)
                        .withPort(REDIS_PORT)
                        .withPassword(REDIS_PASSWORD.toCharArray())
                        .build();
        redisClient = RedisClient.create(redisURI);
        singleConnect = redisClient.connect();
        singleRedisCommands = singleConnect.sync();
        singleRedisCommands.flushdb();
        LOG.info("clear data in redis: {}", REDIS_HOST);
    }

    @AfterEach
    public void stopSingle() {
        singleConnect.close();
        redisClient.shutdown();
    }

    @BeforeEach
    public void cleanCluster() {
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

        clusterClient = RedisClusterClient.create(redisURIS);
        clusterConnection = clusterClient.connect();
        clusterCommands = clusterConnection.sync();
        clusterCommands.flushdb();
        LOG.info("clear data in redis: {}", CLUSTERNODES);
    }

    @AfterEach
    public void stopCluster() {
        clusterConnection.close();
        clusterClient.shutdown();
    }
}
