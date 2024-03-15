package org.apache.flink.streaming.connectors.redis.async;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.ClusterTopologyRefreshOptions;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisAdvancedClusterAsyncCommands;

import org.apache.flink.streaming.connectors.redis.table.base.TestRedisConfigBase;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @Author: Jeff Zou @Date: 2022/10/5 11:20
 */
public class AsyncTest extends TestRedisConfigBase {

    @Test
    public void testLettuceCluster() throws Exception {
        List<RedisURI> redisURIS = new ArrayList<>();
        Arrays.stream(
                        "10.11.0.1:7000,10.11.0.1:7001,10.11.0.1:8000,10.11.0.1:8001,10.11.0.1:9000,10.11.0.1:9001"
                                .split(","))
                .forEach(
                        node -> {
                            String[] redis = node.split(":");
                            RedisURI redisURI =
                                    RedisURI.builder()
                                            .withPassword("abc123".toCharArray())
                                            .withHost(redis[0])
                                            .withPort(Integer.parseInt(redis[1]))
                                            .build();
                            redisURIS.add(redisURI);
                        });

        RedisClusterClient clusterClient = RedisClusterClient.create(redisURIS);

        ClusterTopologyRefreshOptions topologyRefreshOptions =
                ClusterTopologyRefreshOptions.builder()
                        .enableAdaptiveRefreshTrigger(
                                ClusterTopologyRefreshOptions.RefreshTrigger.MOVED_REDIRECT,
                                ClusterTopologyRefreshOptions.RefreshTrigger.PERSISTENT_RECONNECTS)
                        .adaptiveRefreshTriggersTimeout(Duration.ofSeconds(10L))
                        .build();

        clusterClient.setOptions(
                ClusterClientOptions.builder()
                        .topologyRefreshOptions(topologyRefreshOptions)
                        .build());

        StatefulRedisClusterConnection<String, String> connection = clusterClient.connect();
        RedisAdvancedClusterAsyncCommands clusterAsyncCommands = connection.async();
        RedisFuture<String> future = null;
        long start = System.currentTimeMillis();
        for (int i = 0; i < 100000; i++) {
            StringBuilder sb = new StringBuilder();
            String a = sb.append("test").append(i).toString();
            future = clusterAsyncCommands.set(a, a);
        }
        //        future.await(2L, TimeUnit.SECONDS);

        System.out.println(System.currentTimeMillis() - start);
        connection.close();
        clusterClient.shutdown();
    }

    @Test
    public void testLettuceAsync() throws Exception {
        RedisURI redisURI =
                RedisURI.builder()
                        .withHost(REDIS_HOST)
                        .withPassword(REDIS_PASSWORD.toCharArray())
                        .withPort(REDIS_PORT)
                        .build();
        RedisClient redisClient = RedisClient.create(redisURI);
        StatefulRedisConnection<String, String> connection = redisClient.connect();
        RedisAsyncCommands async = connection.async();
        RedisFuture<String> future = null;
        long start = System.currentTimeMillis();

        for (int i = 0; i < 100000; i++) {
            StringBuilder sb = new StringBuilder();
            String a = sb.append("test").append(i).toString();
            future = async.set(a, a);
        }

        future.await(2, TimeUnit.SECONDS);
        connection.close();
        redisClient.shutdown();
    }

    @Test
    public void testLettuceSync() throws Exception {
        RedisURI redisURI =
                RedisURI.builder()
                        .withHost(REDIS_HOST)
                        .withPassword(REDIS_PASSWORD.toCharArray())
                        .withPort(REDIS_PORT)
                        .build();
        RedisClient redisClient = RedisClient.create(redisURI);
        StatefulRedisConnection<String, String> connection = redisClient.connect();
        //        RedisAsyncCommands async = connection.async();
        RedisCommands redisCommands = connection.sync();
        //        RedisFuture<String> future = null;
        long start = System.currentTimeMillis();

        for (int i = 0; i < 100000; i++) {
            StringBuilder sb = new StringBuilder();
            String a = sb.append("test").append(i).toString();
            redisCommands.set(a, a);
        }

        //        future.await(2L, TimeUnit.SECONDS);

        System.out.println(System.currentTimeMillis() - start);
        connection.close();
        redisClient.shutdown();
    }
}
