package org.apache.flink.streaming.connectors.redis.table;

import org.apache.flink.calcite.shaded.com.google.common.base.Preconditions;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisConfigBase;
import org.apache.flink.streaming.connectors.redis.common.config.RedisCacheOptions;
import org.apache.flink.streaming.connectors.redis.common.container.RedisCommandsContainer;
import org.apache.flink.streaming.connectors.redis.common.container.RedisCommandsContainerBuilder;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandBaseDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.streaming.connectors.redis.common.util.RedisSerializeUtil;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.types.DataType;

import org.apache.flink.shaded.guava18.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava18.com.google.common.cache.CacheBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * redis lookup function.
 * @Author: jeff.zou
 * @Date: 2022/3/7.14:33
 */
public class RedisLookupFunction extends TableFunction<RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(RedisLookupFunction.class);

    private RedisCommand redisCommand;
    private FlinkJedisConfigBase flinkJedisConfigBase;
    private RedisCommandsContainer redisCommandsContainer;

    private final long cacheMaxSize;
    private final long cacheTtl;
    private final int maxRetryTimes;
    private final List<DataType> dataTypes;

    private Cache<String, GenericRowData> cache;

    public RedisLookupFunction(
            FlinkJedisConfigBase flinkJedisConfigBase,
            RedisMapper redisMapper,
            RedisCacheOptions redisCacheOptions,
            ResolvedSchema resolvedSchema) {
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
        RedisCommandBaseDescription redisCommandDescription = redisMapper.getCommandDescription();
        this.redisCommand = redisCommandDescription.getRedisCommand();
        Preconditions.checkArgument(
                redisCommand == RedisCommand.HGET || redisCommand == RedisCommand.GET,
                "unsupport command for query redis: %s",
                redisCommand.name());
        this.dataTypes = resolvedSchema.getColumnDataTypes();
    }

    public void eval(Object... keys) throws Exception {
        if (cache != null) {
            GenericRowData genericRowData = null;
            switch (redisCommand) {
                case GET:
                    genericRowData = cache.getIfPresent(String.valueOf(keys[0]));
                    break;
                case HGET:
                    String key =
                            new StringBuilder(String.valueOf(keys[0]))
                                    .append("\01")
                                    .append(String.valueOf(keys[1]))
                                    .toString();
                    genericRowData = cache.getIfPresent(key);
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
                        RedisSerializeUtil.dataTypeFromString(
                                dataTypes.get(1).getLogicalType(), result));
                collect(rowData);
                if (cache != null && result != null) {
                    cache.put(String.valueOf(keys[0]), rowData);
                }
                break;
            case HGET:
                result =
                        this.redisCommandsContainer.hget(
                                String.valueOf(keys[0]), String.valueOf(keys[1]));
                rowData = new GenericRowData(3);
                rowData.setField(0, keys[0]);
                rowData.setField(1, keys[1]);
                rowData.setField(
                        2,
                        RedisSerializeUtil.dataTypeFromString(
                                dataTypes.get(2).getLogicalType(), result));
                collect(rowData);
                if (cache != null && result != null) {
                    String key =
                            new StringBuilder(String.valueOf(keys[0]))
                                    .append("\01")
                                    .append(String.valueOf(keys[1]))
                                    .toString();
                    cache.put(key, rowData);
                }
                break;
            default:
        }
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
