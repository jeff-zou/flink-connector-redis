package org.apache.flink.streaming.connectors.redis.table.base;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author: Jeff Zou @Date: 2022/10/14 10:07
 */
public class TestRedisConfigBase {

    private static final Logger LOG = LoggerFactory.getLogger(TestRedisConfigBase.class);

    public static final String REDIS_HOST = "127.0.0.1";
    public static final int REDIS_PORT = 6379;
    public static final String REDIS_PASSWORD = "";
    protected static StatefulRedisConnection<String, String> singleConnect;
    protected static RedisCommands singleRedisCommands;

    private static RedisClient redisClient;

    @BeforeAll
    public static void connectRedis() {
        RedisURI redisURI =
                RedisURI.builder()
                        .withHost(REDIS_HOST)
                        .withPort(REDIS_PORT)
                        .withPassword(REDIS_PASSWORD.toCharArray())
                        .build();
        redisClient = RedisClient.create(redisURI);
        singleConnect = redisClient.connect();
        singleRedisCommands = singleConnect.sync();
        LOG.info("connect to the redis: {}", REDIS_HOST);
    }

    @AfterAll
    public static void stopSingle() {
        singleConnect.close();
        redisClient.shutdown();
    }

    protected String sigleWith() {
        return "'connector'='redis', "
                + "'host'='"
                + REDIS_HOST
                + "','port'='"
                + REDIS_PORT
                + "', 'redis-mode'='single','password'='"
                + REDIS_PASSWORD
                + "',";
    }
}
