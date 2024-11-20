/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.redis.config;

/** query options. @Author:Jeff.Zou @Date: 2022/3/9.14:37 */
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
