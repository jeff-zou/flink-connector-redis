package org.apache.flink.streaming.connectors.redis.table;

import static org.apache.flink.streaming.connectors.redis.table.RedisDynamicTableFactory.CACHE_SEPERATOR;

import org.apache.flink.calcite.shaded.com.google.common.cache.Cache;
import org.apache.flink.calcite.shaded.com.google.common.cache.CacheBuilder;
import org.apache.flink.streaming.connectors.redis.command.RedisCommand;
import org.apache.flink.streaming.connectors.redis.command.RedisCommandBaseDescription;
import org.apache.flink.streaming.connectors.redis.config.FlinkConfigBase;
import org.apache.flink.streaming.connectors.redis.config.RedisQueryOptions;
import org.apache.flink.streaming.connectors.redis.config.RedisValueDataStructure;
import org.apache.flink.streaming.connectors.redis.container.RedisCommandsContainer;
import org.apache.flink.streaming.connectors.redis.container.RedisCommandsContainerBuilder;
import org.apache.flink.streaming.connectors.redis.mapper.RedisMapper;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/** redis lookup function. @Author: jeff.zou @Date: 2022/3/7.14:33 */
public class RedisLookupFunction extends AsyncTableFunction<RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(RedisLookupFunction.class);

    private RedisCommand redisCommand;
    private FlinkConfigBase flinkConfigBase;
    private RedisCommandsContainer redisCommandsContainer;

    private final long cacheMaxSize;
    private final long cacheTtl;
    private final int maxRetryTimes;
    private final List<DataType> dataTypes;
    private final boolean loadAll;
    private final RedisValueDataStructure redisValueDataStructure;
    private Cache<String, Object> cache;

    public RedisLookupFunction(
            FlinkConfigBase flinkConfigBase,
            RedisMapper redisMapper,
            RedisQueryOptions redisQueryOptions,
            ResolvedSchema resolvedSchema) {
        Preconditions.checkNotNull(
                flinkConfigBase, "Redis connection pool config should not be null");
        Preconditions.checkNotNull(redisMapper, "Redis Mapper can not be null");

        this.flinkConfigBase = flinkConfigBase;
        this.cacheTtl = redisQueryOptions.getCacheTtl();
        this.cacheMaxSize = redisQueryOptions.getCacheMaxSize();
        this.maxRetryTimes = redisQueryOptions.getMaxRetryTimes();
        this.loadAll = redisQueryOptions.getLoadAll();
        this.redisValueDataStructure = redisQueryOptions.getRedisValueDataStructure();

        if (this.loadAll) {
            Preconditions.checkArgument(
                    cacheMaxSize != -1 && cacheTtl != -1,
                    "cache must be opened by cacheMaxSize and cacheTtl when u want to load all elements to cache.");
            Preconditions.checkArgument(
                    this.loadAll && redisCommand == RedisCommand.HSET,
                    "just hget support load all.");
        }

        RedisCommandBaseDescription redisCommandDescription = redisMapper.getCommandDescription();
        Preconditions.checkNotNull(
                redisCommandDescription, "Redis Mapper data type description can not be null");
        this.redisCommand = redisCommandDescription.getRedisCommand();
        Preconditions.checkArgument(
                redisCommand == RedisCommand.HGET || redisCommand == RedisCommand.GET,
                "unsupport command for query redis: %s, just get hget.",
                redisCommand.name());

        this.dataTypes = resolvedSchema.getColumnDataTypes();
    }

    public void eval(CompletableFuture<Collection<GenericRowData>> resultFuture, Object... keys)
            throws Exception {

        // when use cache.
        if (cache != null) {
            GenericRowData genericRowData = null;
            switch (redisCommand) {
                case GET:
                    genericRowData = (GenericRowData) cache.getIfPresent(String.valueOf(keys[0]));
                    break;
                case HGET:
                    if (loadAll) {
                        Map<String, String> map =
                                (Map<String, String>) cache.getIfPresent(String.valueOf(keys[0]));
                        if (map != null) {
                            resultFuture.complete(
                                    Collections.singleton(
                                            RedisResultWrapper.createRowDataForHash(
                                                    keys,
                                                    map.get(String.valueOf(keys[1])),
                                                    redisValueDataStructure,
                                                    dataTypes)));
                            return;
                        }
                    } else {
                        String key =
                                new StringBuilder(String.valueOf(keys[0]))
                                        .append(CACHE_SEPERATOR)
                                        .append(String.valueOf(keys[1]))
                                        .toString();
                        genericRowData = (GenericRowData) cache.getIfPresent(key);
                    }
                    break;
                default:
            }

            // when cache is not null.
            if (genericRowData != null) {
                resultFuture.complete(Collections.singleton(genericRowData));
                return;
            }
        }

        // It will try many times which less than {@code maxRetryTimes} until execute success.
        for (int i = 0; i <= maxRetryTimes; i++) {
            try {
                query(resultFuture, keys);
                break;
            } catch (Exception e) {
                LOG.error("query redis error, retry times:{}", i, e);
                if (i >= maxRetryTimes) {
                    throw new RuntimeException("query redis error ", e);
                }
                Thread.sleep(500 * i);
            }
        }
    }

    /**
     * query redis.
     *
     * @param keys
     * @throws Exception
     */
    private void query(CompletableFuture<Collection<GenericRowData>> resultFuture, Object... keys) {
        switch (redisCommand) {
            case GET:
                this.redisCommandsContainer
                        .get(String.valueOf(keys[0]))
                        .thenAccept(
                                result -> {
                                    GenericRowData rowData =
                                            RedisResultWrapper.createRowDataForString(
                                                    keys,
                                                    result,
                                                    redisValueDataStructure,
                                                    dataTypes);
                                    resultFuture.complete(Collections.singleton(rowData));
                                    if (cache != null && result != null) {
                                        cache.put(String.valueOf(keys[0]), rowData);
                                    }
                                });

                break;
            case HGET:
                if (loadAll) {
                    loadAllElements(resultFuture, keys);
                    return;
                }

                this.redisCommandsContainer
                        .hget(String.valueOf(keys[0]), String.valueOf(keys[1]))
                        .thenAccept(
                                result -> {
                                    GenericRowData rowData =
                                            RedisResultWrapper.createRowDataForHash(
                                                    keys,
                                                    result,
                                                    redisValueDataStructure,
                                                    dataTypes);
                                    resultFuture.complete(Collections.singleton(rowData));
                                    if (cache != null && result != null) {
                                        String key =
                                                new StringBuilder(String.valueOf(keys[0]))
                                                        .append(CACHE_SEPERATOR)
                                                        .append(String.valueOf(keys[1]))
                                                        .toString();
                                        cache.put(key, rowData);
                                    }
                                });

                break;
            default:
        }
    }

    /**
     * load all element in memory from map.
     *
     * @param keys
     */
    private void loadAllElements(
            CompletableFuture<Collection<GenericRowData>> resultFuture, Object... keys) {
        this.redisCommandsContainer
                .hgetAll(String.valueOf(keys[0]))
                .thenAccept(
                        map -> {
                            cache.put(String.valueOf(keys[0]), map);
                            resultFuture.complete(
                                    Collections.singleton(
                                            RedisResultWrapper.createRowDataForHash(
                                                    keys,
                                                    map.get(String.valueOf(keys[1])),
                                                    redisValueDataStructure,
                                                    dataTypes)));
                        });
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        try {

            this.redisCommandsContainer = RedisCommandsContainerBuilder.build(this.flinkConfigBase);
            this.redisCommandsContainer.open();
            LOG.info("{} success to create redis container:{}", Thread.currentThread().getId());
        } catch (Exception e) {
            LOG.error("Redis has not been properly initialized: ", e);
            throw e;
        }

        this.cache =
                cacheMaxSize == -1 || cacheTtl == -1
                        ? null
                        : CacheBuilder.newBuilder()
                                .expireAfterWrite(cacheTtl, TimeUnit.SECONDS)
                                .maximumSize(cacheMaxSize)
                                .build();
    }

    @Override
    public void close() throws Exception {
        if (redisCommandsContainer != null) {
            redisCommandsContainer.close();
        }

        if (cache != null) {
            cache.cleanUp();
            cache = null;
        }
    }
}
