package org.apache.flink.streaming.connectors.redis.table.base;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author: Jeff Zou @Date: 2022/10/14 10:07
 */
public class TestRedisConfigBase {

    private static final Logger LOG = LoggerFactory.getLogger(TestRedisConfigBase.class);

    public static final String REDIS_HOST = "10.11.69.176";
    public static final int REDIS_PORT = 6379;
    public static final String REDIS_PASSWORD = "******";
    protected StatefulRedisConnection<String, String> singleConnect;
    protected RedisCommands singleRedisCommands;

    private RedisClient redisClient;

    @BeforeEach
    public void connectRedis() {
        RedisURI redisURI =
                RedisURI.builder()
                        .withHost(REDIS_HOST)
                        .withPort(REDIS_PORT)
                        .withPassword(REDIS_PASSWORD.toCharArray())
                        .build();
        redisClient = RedisClient.create(redisURI);
        singleConnect = redisClient.connect();
        singleRedisCommands = singleConnect.sync();
        LOG.info("connecto to the redis: {}", REDIS_HOST);
    }

    @AfterEach
    public void stopSingle() {
        singleConnect.close();
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
