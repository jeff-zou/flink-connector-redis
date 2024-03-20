package org.apache.flink.streaming.connectors.redis.config;

/** query options. @Author:jeff.zou @Date: 2022/3/9.14:37 */
public class RedisJoinConfig {

    private final long cacheMaxSize;
    private final long cacheTtl;
    private final boolean loadAll;

    public RedisJoinConfig(long cacheMaxSize, long cacheTtl, boolean loadAll) {
        this.cacheMaxSize = cacheMaxSize;
        this.cacheTtl = cacheTtl;
        this.loadAll = loadAll;
    }

    public long getCacheMaxSize() {
        return cacheMaxSize;
    }

    public long getCacheTtl() {
        return cacheTtl;
    }

    public boolean getLoadAll() {
        return loadAll;
    }

    /** */
    public static class Builder {
        private long cacheMaxSize = -1L;
        private long cacheTtl = -1L;
        private boolean loadAll = false;

        public Builder setCacheMaxSize(long cacheMaxSize) {
            this.cacheMaxSize = cacheMaxSize;
            return this;
        }

        public Builder setCacheTTL(long cacheTtl) {
            this.cacheTtl = cacheTtl;
            return this;
        }

        public Builder setLoadAll(boolean loadAll) {
            this.loadAll = loadAll;
            return this;
        }

        public RedisJoinConfig build() {
            return new RedisJoinConfig(cacheMaxSize, cacheTtl, loadAll);
        }
    }
}
