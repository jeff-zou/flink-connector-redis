package org.apache.flink.streaming.connectors.redis.common.config;

/** query options. @Author:jeff.zou @Date: 2022/3/9.14:37 */
public class RedisLookupOptions {

    private final long cacheMaxSize;
    private final long cacheTtl;
    private final int maxRetryTimes;
    private final boolean loadAll;
    private final RedisValueDataStructure redisValueDataStructure;

    public RedisLookupOptions(
            long cacheMaxSize,
            long cacheTtl,
            int maxRetryTimes,
            boolean loadAll,
            RedisValueDataStructure redisValueDataStructure) {
        this.cacheMaxSize = cacheMaxSize;
        this.cacheTtl = cacheTtl;
        this.maxRetryTimes = maxRetryTimes;
        this.loadAll = loadAll;
        this.redisValueDataStructure = redisValueDataStructure;
    }

    public long getCacheMaxSize() {
        return cacheMaxSize;
    }

    public long getCacheTtl() {
        return cacheTtl;
    }

    public int getMaxRetryTimes() {
        return maxRetryTimes;
    }

    public boolean getLoadAll() {
        return loadAll;
    }

    public RedisValueDataStructure getRedisValueDataStructure() {
        return redisValueDataStructure;
    }

    /** */
    public static class Builder {
        private long cacheMaxSize = -1L;
        private long cacheTtl = -1L;
        private int maxRetryTimes = 1;
        private boolean loadAll = false;
        private RedisValueDataStructure redisValueDataStructure =
                RedisOptions.VALUE_DATA_STRUCTURE.defaultValue();

        public Builder setCacheMaxSize(long cacheMaxSize) {
            this.cacheMaxSize = cacheMaxSize;
            return this;
        }

        public Builder setCacheTTL(long cacheTtl) {
            this.cacheTtl = cacheTtl;
            return this;
        }

        public Builder setMaxRetryTimes(int maxRetryTimes) {
            this.maxRetryTimes = maxRetryTimes;
            return this;
        }

        public Builder setLoadAll(boolean loadAll) {
            this.loadAll = loadAll;
            return this;
        }

        public Builder setRedisValueDataStructure(RedisValueDataStructure redisValueDataStructure) {
            this.redisValueDataStructure = redisValueDataStructure;
            return this;
        }

        public RedisLookupOptions build() {
            return new RedisLookupOptions(
                    cacheMaxSize, cacheTtl, maxRetryTimes, loadAll, redisValueDataStructure);
        }
    }
}
