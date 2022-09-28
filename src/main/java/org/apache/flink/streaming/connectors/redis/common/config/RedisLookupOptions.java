package org.apache.flink.streaming.connectors.redis.common.config;

/** query options. @Author:jeff.zou @Date: 2022/3/9.14:37 */
public class RedisLookupOptions {

    private final long cacheMaxSize;
    private final long cacheTtl;
    private final int maxRetryTimes;
    private final boolean loadAll;

    public RedisLookupOptions(
            long cacheMaxSize, long cacheTtl, int maxRetryTimes, boolean loadAll) {
        this.cacheMaxSize = cacheMaxSize;
        this.cacheTtl = cacheTtl;
        this.maxRetryTimes = maxRetryTimes;
        this.loadAll = loadAll;
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

    @Override
    public String toString() {
        return "RedisCacheOptions{"
                + "cacheMaxSize="
                + cacheMaxSize
                + ", cacheTtl="
                + cacheTtl
                + ", maxRetryTimes="
                + maxRetryTimes
                + ", loadAll="
                + loadAll
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        RedisLookupOptions that = (RedisLookupOptions) o;

        if (cacheMaxSize != that.cacheMaxSize) {
            return false;
        }
        if (cacheTtl != that.cacheTtl) {
            return false;
        }
        return maxRetryTimes == that.maxRetryTimes;
    }

    @Override
    public int hashCode() {
        int result = (int) (cacheMaxSize ^ (cacheMaxSize >>> 32));
        result = 31 * result + (int) (cacheTtl ^ (cacheTtl >>> 32));
        result = 31 * result + maxRetryTimes;
        return result;
    }

    /** */
    public static class Builder {
        private long cacheMaxSize = -1L;
        private long cacheTtl = -1L;
        private int maxRetryTimes = 1;
        private boolean loadAll = false;

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

        public RedisLookupOptions build() {
            return new RedisLookupOptions(cacheMaxSize, cacheTtl, maxRetryTimes, loadAll);
        }
    }
}
