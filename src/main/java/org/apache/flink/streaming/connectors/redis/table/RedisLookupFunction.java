package org.apache.flink.streaming.connectors.redis.table;

import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisConfigBase;
import org.apache.flink.streaming.connectors.redis.common.config.RedisCacheOptions;
import org.apache.flink.streaming.connectors.redis.common.container.RedisCommandsContainer;
import org.apache.flink.streaming.connectors.redis.common.container.RedisCommandsContainerBuilder;
import org.apache.flink.streaming.connectors.redis.common.converter.RedisRowConverter;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandBaseDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.guava18.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava18.com.google.common.cache.CacheBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.streaming.connectors.redis.table.RedisDynamicTableFactory.CACHE_SEPERATOR;

/** redis lookup function. @Author: jeff.zou @Date: 2022/3/7.14:33 */
public class RedisLookupFunction extends TableFunction<RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(RedisLookupFunction.class);

    private RedisCommand redisCommand;
    private FlinkJedisConfigBase flinkJedisConfigBase;
    private RedisCommandsContainer redisCommandsContainer;

    private final long cacheMaxSize;
    private final long cacheTtl;
    private final int maxRetryTimes;
    private final DataType[] dataTypes;
    private final boolean loadAll;
    private Cache<String, Object> cache;

    public RedisLookupFunction(
            FlinkJedisConfigBase flinkJedisConfigBase,
            RedisMapper redisMapper,
            RedisCacheOptions redisCacheOptions,
            TableSchema tableSchema) {
        Preconditions.checkNotNull(
                flinkJedisConfigBase, "Redis connection pool config should not be null");
        Preconditions.checkNotNull(redisMapper, "Redis Mapper can not be null");
        Preconditions.checkNotNull(
                redisMapper.getCommandDescription(),
                "Redis Mapper data type description can not be null");

        this.flinkJedisConfigBase = flinkJedisConfigBase;
        this.cacheTtl = redisCacheOptions.getCacheTtl();
        this.cacheMaxSize = redisCacheOptions.getCacheMaxSize();
        this.maxRetryTimes = redisCacheOptions.getMaxRetryTimes();
        this.loadAll = redisCacheOptions.getLoadAll();
        if (this.loadAll) {
            Preconditions.checkState(
                    cacheMaxSize != -1 && cacheTtl != -1,
                    "cache must be opened by cacheMaxSize and cacheTtl when u want to load all elements to cache.");
        }

        RedisCommandBaseDescription redisCommandDescription = redisMapper.getCommandDescription();
        this.redisCommand = redisCommandDescription.getRedisCommand();
        Preconditions.checkArgument(
                redisCommand == RedisCommand.HGET || redisCommand == RedisCommand.GET,
                "unsupport command for query redis: %s",
                redisCommand.name());
        this.dataTypes = tableSchema.getFieldDataTypes();
    }

    public void eval(Object... keys) throws Exception {
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
                            createRowData(keys, map);
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
            if (genericRowData != null) {
                collect(genericRowData);
                return;
            }
        }

        for (int i = 0; i <= maxRetryTimes; i++) {
            try {
                query(keys);
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

    private void query(Object... keys) throws Exception {
        String result = null;
        GenericRowData rowData = null;
        switch (redisCommand) {
            case GET:
                result = this.redisCommandsContainer.get(String.valueOf(keys[0]));
                rowData = new GenericRowData(2);
                rowData.setField(0, keys[0]);
                rowData.setField(
                        1,
                        RedisRowConverter.dataTypeFromString(
                                dataTypes[1].getLogicalType(), result));
                collect(rowData);
                if (cache != null && result != null) {
                    cache.put(String.valueOf(keys[0]), rowData);
                }
                break;
            case HGET:
                if (loadAll) {
                    hgetAll(keys);
                    return;
                }
                result =
                        this.redisCommandsContainer.hget(
                                String.valueOf(keys[0]), String.valueOf(keys[1]));
                rowData = new GenericRowData(3);
                rowData.setField(0, keys[0]);
                rowData.setField(1, keys[1]);
                rowData.setField(
                        2,
                        RedisRowConverter.dataTypeFromString(
                                dataTypes[2].getLogicalType(), result));
                collect(rowData);
                if (cache != null && result != null) {
                    String key =
                            new StringBuilder(String.valueOf(keys[0]))
                                    .append(CACHE_SEPERATOR)
                                    .append(String.valueOf(keys[1]))
                                    .toString();
                    cache.put(key, rowData);
                }
                break;
            default:
        }
    }

    void hgetAll(Object... keys) {
        Map<String, String> map = this.redisCommandsContainer.hgetAll(String.valueOf(keys[0]));
        if (map == null) {
            return;
        }

        cache.put(String.valueOf(keys[0]), map);
        createRowData(keys, map);
    }

    void createRowData(Object[] keys, Map<String, String> map) {
        GenericRowData genericRowData = new GenericRowData(3);
        genericRowData.setField(0, keys[0]);
        genericRowData.setField(1, keys[1]);
        genericRowData.setField(
                2,
                RedisRowConverter.dataTypeFromString(
                        dataTypes[2].getLogicalType(), map.get(String.valueOf(keys[1]))));
        collect(genericRowData);
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        try {
            this.redisCommandsContainer =
                    RedisCommandsContainerBuilder.build(this.flinkJedisConfigBase);
            this.redisCommandsContainer.open();
            LOG.info("success to create redis container:{}", this.flinkJedisConfigBase.toString());
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
