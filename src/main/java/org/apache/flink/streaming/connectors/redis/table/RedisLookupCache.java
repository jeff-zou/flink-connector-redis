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

package org.apache.flink.streaming.connectors.redis.table;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.Preconditions;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/**
 * Bounded cache with expire-after-write semantics, used by {@link RedisLookupFunction} to hold
 * looked up rows.
 *
 * <p>This replaces the guava cache that used to be imported from {@code
 * org.apache.flink.calcite.shaded.com.google.common.cache}. That package is an internal shading
 * artifact of flink-table-planner rather than public api, and since flink 1.15 the planner sits
 * behind flink-table-planner-loader and its isolated class loader, so it cannot be loaded from user
 * code at all. Implementing the two methods the lookup function actually needs avoids both the
 * broken dependency and adding a guava of our own to the connector jar.
 *
 * <p>Entries expire once {@code ttlSeconds} have passed since they were written, matching guava's
 * {@code expireAfterWrite}. Once {@code maximumSize} is exceeded the least recently used entry is
 * evicted, matching guava's {@code maximumSize}. Reading an entry does not extend its lifetime.
 *
 * <p>All methods are thread safe: {@link #getIfPresent} is called from the task thread inside {@code
 * eval}, while {@link #put} is called from lettuce's netty event loop threads when a {@code
 * RedisFuture} completes.
 */
public class RedisLookupCache {

    private final long ttlNanos;

    private final LongSupplier nanoClock;

    private final LinkedHashMap<String, CacheEntry> entries;

    public RedisLookupCache(long ttlSeconds, long maximumSize) {
        this(ttlSeconds, maximumSize, System::nanoTime);
    }

    @VisibleForTesting
    RedisLookupCache(long ttlSeconds, long maximumSize, LongSupplier nanoClock) {
        Preconditions.checkArgument(ttlSeconds >= 0, "cache ttl can not be negative");
        Preconditions.checkArgument(maximumSize >= 0, "cache maximum size can not be negative");
        this.ttlNanos = TimeUnit.SECONDS.toNanos(ttlSeconds);
        this.nanoClock = nanoClock;
        // access ordered so that removeEldestEntry evicts the least recently used entry
        this.entries =
                new LinkedHashMap<String, CacheEntry>(16, 0.75f, true) {

                    private static final long serialVersionUID = 1L;

                    @Override
                    protected boolean removeEldestEntry(Map.Entry<String, CacheEntry> eldest) {
                        return size() > maximumSize;
                    }
                };
    }

    /**
     * Returns the value cached under the given key, or null when it was never cached or has expired.
     *
     * @param key cache key
     * @return the cached value, or null
     */
    public synchronized Object getIfPresent(String key) {
        CacheEntry entry = entries.get(key);
        if (entry == null) {
            return null;
        }

        if (nanoClock.getAsLong() - entry.writeNanos >= ttlNanos) {
            entries.remove(key);
            return null;
        }

        return entry.value;
    }

    /**
     * Caches the value under the given key, restarting its time to live.
     *
     * @param key cache key
     * @param value value to cache
     */
    public synchronized void put(String key, Object value) {
        entries.put(key, new CacheEntry(value, nanoClock.getAsLong()));
    }

    /** Drops every entry, releasing the cached rows on close. */
    public synchronized void clear() {
        entries.clear();
    }

    /**
     * Returns the number of entries held, including any that have expired but have not been read
     * again since.
     *
     * @return number of entries held
     */
    @VisibleForTesting
    synchronized int size() {
        return entries.size();
    }

    private static final class CacheEntry {

        private final Object value;

        private final long writeNanos;

        private CacheEntry(Object value, long writeNanos) {
            this.value = value;
            this.writeNanos = writeNanos;
        }
    }
}
