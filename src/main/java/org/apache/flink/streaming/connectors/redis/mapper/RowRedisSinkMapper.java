package org.apache.flink.streaming.connectors.redis.mapper;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.connectors.redis.command.RedisCommand;
import org.apache.flink.streaming.connectors.redis.command.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.config.RedisOptions;
import org.apache.flink.streaming.connectors.redis.converter.RedisRowConverter;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.StringUtils;

import java.time.LocalTime;

/** base row redis mapper implement. */
public class RowRedisSinkMapper implements RedisSinkMapper<GenericRowData> {

    private Integer ttl;

    private LocalTime expireTime;

    private RedisCommand redisCommand;

    private Boolean setIfAbsent;

    private Boolean ttlKeyNotAbsent;

    public RowRedisSinkMapper(RedisCommand redisCommand, ReadableConfig config) {
        this.redisCommand = redisCommand;
        this.ttl = config.get(RedisOptions.TTL);
        this.setIfAbsent = config.get(RedisOptions.SET_IF_ABSENT);
        this.ttlKeyNotAbsent = config.get(RedisOptions.TTL_KEY_NOT_ABSENT);
        String expireOnTime = config.get(RedisOptions.EXPIRE_ON_TIME);
        if (!StringUtils.isNullOrWhitespaceOnly(expireOnTime)) {
            this.expireTime = LocalTime.parse(expireOnTime);
        }
    }

    @Override
    public RedisCommandDescription getCommandDescription() {
        return new RedisCommandDescription(
                redisCommand, ttl, expireTime, setIfAbsent, ttlKeyNotAbsent);
    }

    @Override
    public String getKeyFromData(RowData rowData, LogicalType logicalType, Integer keyIndex) {
        return RedisRowConverter.rowDataToString(logicalType, rowData, keyIndex);
    }

    @Override
    public String getValueFromData(RowData rowData, LogicalType logicalType, Integer valueIndex) {
        return RedisRowConverter.rowDataToString(logicalType, rowData, valueIndex);
    }

    @Override
    public String getFieldFromData(RowData rowData, LogicalType logicalType, Integer fieldIndex) {
        return RedisRowConverter.rowDataToString(logicalType, rowData, fieldIndex);
    }

    public RedisCommand getRedisCommand() {
        return redisCommand;
    }

    @Override
    public boolean equals(Object obj) {
        RedisCommand redisCommand = ((RowRedisSinkMapper) obj).redisCommand;
        return this.redisCommand == redisCommand;
    }
}
