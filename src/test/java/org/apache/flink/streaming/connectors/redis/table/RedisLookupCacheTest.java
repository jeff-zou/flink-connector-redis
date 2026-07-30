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

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link RedisLookupCache}, the replacement for the guava cache that used to come from the
 * shaded calcite inside flink-table-planner. Time is injected, so no test sleeps.
 */
public class RedisLookupCacheTest {

    /** Manually advanced nano clock, so expiry can be tested without sleeping. */
    private static final class FakeClock {

        private final AtomicLong nanos = new AtomicLong();

        private long get() {
            return nanos.get();
        }

        private void advanceSeconds(long seconds) {
            nanos.addAndGet(TimeUnit.SECONDS.toNanos(seconds));
        }
    }

    @Test
    public void testPutThenGet() {
        RedisLookupCache cache = new RedisLookupCache(10, 100);

        cache.put("k", "v");

        assertEquals("v", cache.getIfPresent("k"));
    }

    @Test
    public void testMissReturnsNull() {
        RedisLookupCache cache = new RedisLookupCache(10, 100);

        assertNull(cache.getIfPresent("absent"));
    }

    @Test
    public void testEntryExpiresAfterTtl() {
        FakeClock clock = new FakeClock();
        RedisLookupCache cache = new RedisLookupCache(10, 100, clock::get);

        cache.put("k", "v");
        clock.advanceSeconds(9);
        assertEquals("v", cache.getIfPresent("k"), "must still be cached just before the ttl");

        clock.advanceSeconds(1);
        assertNull(cache.getIfPresent("k"), "must be gone once the ttl has elapsed");
    }

    @Test
    public void testReadDoesNotExtendTtl() {
        FakeClock clock = new FakeClock();
        RedisLookupCache cache = new RedisLookupCache(10, 100, clock::get);

        cache.put("k", "v");
        // read repeatedly across the whole window; expireAfterWrite must not be refreshed by a read
        for (int i = 0; i < 9; i++) {
            clock.advanceSeconds(1);
            assertEquals("v", cache.getIfPresent("k"));
        }

        clock.advanceSeconds(1);
        assertNull(cache.getIfPresent("k"), "ttl is counted from the write, not from the last read");
    }

    @Test
    public void testRewriteRestartsTtl() {
        FakeClock clock = new FakeClock();
        RedisLookupCache cache = new RedisLookupCache(10, 100, clock::get);

        cache.put("k", "v1");
        clock.advanceSeconds(9);
        cache.put("k", "v2");
        clock.advanceSeconds(9);

        assertEquals("v2", cache.getIfPresent("k"));
    }

    @Test
    public void testExpiredEntryIsRemovedNotJustHidden() {
        FakeClock clock = new FakeClock();
        RedisLookupCache cache = new RedisLookupCache(10, 100, clock::get);

        cache.put("k", "v");
        clock.advanceSeconds(10);

        assertEquals(1, cache.size());
        assertNull(cache.getIfPresent("k"));
        assertEquals(0, cache.size(), "reading an expired entry must drop it");
    }

    @Test
    public void testSizeIsBounded() {
        RedisLookupCache cache = new RedisLookupCache(10, 3);

        for (int i = 0; i < 100; i++) {
            cache.put("k" + i, "v" + i);
        }

        assertEquals(3, cache.size());
    }

    @Test
    public void testLeastRecentlyUsedEntryIsEvicted() {
        RedisLookupCache cache = new RedisLookupCache(10, 2);

        cache.put("a", "va");
        cache.put("b", "vb");
        // touching "a" makes "b" the least recently used one
        assertEquals("va", cache.getIfPresent("a"));
        cache.put("c", "vc");

        assertEquals("va", cache.getIfPresent("a"));
        assertEquals("vc", cache.getIfPresent("c"));
        assertNull(cache.getIfPresent("b"), "the least recently used entry must be evicted");
    }

    @Test
    public void testZeroMaximumSizeCachesNothing() {
        RedisLookupCache cache = new RedisLookupCache(10, 0);

        cache.put("k", "v");

        assertNull(cache.getIfPresent("k"));
        assertEquals(0, cache.size());
    }

    @Test
    public void testZeroTtlExpiresImmediately() {
        RedisLookupCache cache = new RedisLookupCache(0, 100);

        cache.put("k", "v");

        assertNull(cache.getIfPresent("k"));
    }

    @Test
    public void testClearDropsEverything() {
        RedisLookupCache cache = new RedisLookupCache(10, 100);
        cache.put("a", "va");
        cache.put("b", "vb");

        cache.clear();

        assertEquals(0, cache.size());
        assertNull(cache.getIfPresent("a"));
    }

    @Test
    public void testNegativeArgumentsAreRejected() {
        assertThrows(IllegalArgumentException.class, () -> new RedisLookupCache(-1, 100));
        assertThrows(IllegalArgumentException.class, () -> new RedisLookupCache(10, -1));
    }

    /**
     * getIfPresent runs on the task thread while put runs on lettuce's netty event loop threads, so
     * the cache has to survive concurrent access the way the guava cache did.
     */
    @Test
    public void testConcurrentPutAndGet() throws Exception {
        RedisLookupCache cache = new RedisLookupCache(60, 64);
        int threads = 8;
        int iterations = 5_000;

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        List<Future<?>> futures = new ArrayList<>();

        try {
            for (int t = 0; t < threads; t++) {
                final int id = t;
                futures.add(
                        pool.submit(
                                () -> {
                                    try {
                                        start.await();
                                        for (int i = 0; i < iterations; i++) {
                                            String key = "k" + ((id * iterations + i) % 128);
                                            cache.put(key, "v" + i);
                                            cache.getIfPresent(key);
                                        }
                                    } catch (Throwable e) {
                                        failure.compareAndSet(null, e);
                                    }
                                }));
            }

            start.countDown();
            for (Future<?> future : futures) {
                future.get(60, TimeUnit.SECONDS);
            }
        } finally {
            pool.shutdownNow();
        }

        assertNull(failure.get(), "concurrent access must not throw");
        assertTrue(cache.size() <= 64, "the size bound must hold under concurrency");
    }
}
