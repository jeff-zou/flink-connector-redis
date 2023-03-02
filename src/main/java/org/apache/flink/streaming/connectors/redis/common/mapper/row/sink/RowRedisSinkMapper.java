package org.apache.flink.streaming.connectors.redis.common.mapper.row.sink;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.connectors.redis.common.config.RedisOptions;
import org.apache.flink.streaming.connectors.redis.common.converter.RedisRowConverter;
import org.apache.flink.streaming.connectors.redis.common.hanlder.RedisMapperHandler;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisSinkMapper;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.StringUtils;

import java.time.LocalTime;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.streaming.connectors.redis.descriptor.RedisValidator.REDIS_COMMAND;

/** base row redis mapper implement. */
public abstract class RowRedisSinkMapper
        implements RedisSinkMapper<GenericRowData>, RedisMapperHandler {

    private Integer ttl;

    private LocalTime expireTime;

    private RedisCommand redisCommand;

    public RowRedisSinkMapper(int ttl, RedisCommand redisCommand) {
        this.ttl = ttl;
        this.redisCommand = redisCommand;
    }

    public RowRedisSinkMapper(RedisCommand redisCommand, int ttl) {
        this.ttl = ttl;
        this.redisCommand = redisCommand;
    }

    public RowRedisSinkMapper(RedisCommand redisCommand) {
        this.redisCommand = redisCommand;
    }

    public RowRedisSinkMapper(RedisCommand redisCommand, Map<String, String> config) {
        this.redisCommand = redisCommand;
    }

    public RowRedisSinkMapper(RedisCommand redisCommand, ReadableConfig config) {
        this.redisCommand = redisCommand;
        this.ttl = config.get(RedisOptions.TTL);
        String expireOnTime = config.get(RedisOptions.EXPIRE_ON_TIME);
        if (!StringUtils.isNullOrWhitespaceOnly(expireOnTime)) {
            this.expireTime = LocalTime.parse(expireOnTime);
        }
    }

    @Override
    public RedisCommandDescription getCommandDescription() {
        return new RedisCommandDescription(redisCommand, ttl, expireTime);
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
    public Map<String, String> requiredContext() {
        Map<String, String> require = new HashMap<>();
        require.put(REDIS_COMMAND, getRedisCommand().name());
        return require;
    }

    @Override
    public boolean equals(Object obj) {
        RedisCommand redisCommand = ((RowRedisSinkMapper) obj).redisCommand;
        return this.redisCommand == redisCommand;
    }
}
