package org.apache.flink.streaming.connectors.redis.common.config;

/**
 * @author jeff.zou
 * @date 2022/3/9.14:37
 */
public class RedisCacheOptions {

    private final long cacheMaxSize;
    private final long cacheTtl;
    private final int maxRetryTimes;

    public RedisCacheOptions(long cacheMaxSize, long cacheTtl, int maxRetryTimes) {
        this.cacheMaxSize = cacheMaxSize;
        this.cacheTtl = cacheTtl;
        this.maxRetryTimes = maxRetryTimes;
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

    @Override
    public String toString() {
        return "RedisCacheOptions{" +
                "cacheMaxSize=" + cacheMaxSize +
                ", cacheTtl=" + cacheTtl +
                ", maxRetryTimes=" + maxRetryTimes +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        RedisCacheOptions that = (RedisCacheOptions) o;

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

    public static class Builder {
        private long cacheMaxSize = -1L;
        private long cacheTtl = -1L;
        private int maxRetryTimes = 1;

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

        public RedisCacheOptions build() {
            return  new RedisCacheOptions(cacheMaxSize, cacheTtl, maxRetryTimes);
        }
    }
}