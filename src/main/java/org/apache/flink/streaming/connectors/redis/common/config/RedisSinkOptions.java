package org.apache.flink.streaming.connectors.redis.common.config;

/** sink options. @Author: Jeff Zou @Date: 2022/9/28 16:36 */
public class RedisSinkOptions {
    private final int maxRetryTimes;

    private final RedisValueFromType redisValueFromType;

    public int getMaxRetryTimes() {
        return maxRetryTimes;
    }

    public RedisValueFromType getRedisValueFromType() {
        return redisValueFromType;
    }

    public RedisSinkOptions(int maxRetryTimes, RedisValueFromType redisValueFromType) {
        this.maxRetryTimes = maxRetryTimes;
        this.redisValueFromType = redisValueFromType;
    }

    public static class Builder {
        private int maxRetryTimes;

        private RedisValueFromType redisValueFromType;

        public Builder setMaxRetryTimes(int maxRetryTimes) {
            this.maxRetryTimes = maxRetryTimes;
            return this;
        }

        public Builder setRedisValueFromType(RedisValueFromType redisValueFromType) {
            this.redisValueFromType = redisValueFromType;
            return this;
        }

        public RedisSinkOptions build() {
            return new RedisSinkOptions(maxRetryTimes, redisValueFromType);
        }
    }
}
