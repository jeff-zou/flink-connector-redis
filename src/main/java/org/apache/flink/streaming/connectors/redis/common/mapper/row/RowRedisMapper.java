package org.apache.flink.streaming.connectors.redis.common.mapper.row;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.connectors.redis.common.config.RedisOptions;
import org.apache.flink.streaming.connectors.redis.common.hanlder.RedisMapperHandler;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.table.data.GenericRowData;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.streaming.connectors.redis.descriptor.RedisValidator.REDIS_COMMAND;

/**
 * base row redis mapper implement.
 */
public abstract class RowRedisMapper implements RedisMapper<GenericRowData>, RedisMapperHandler {

    private Integer ttl;

    private RedisCommand redisCommand;

    private String fieldColumn;

    private String keyColumn;

    private String valueColumn;

    private boolean putIfAbsent;

    public RowRedisMapper(int ttl, RedisCommand redisCommand, String keyColumn, String valueColumn, boolean putIfAbsent) {
        this.ttl = ttl;
        this.redisCommand = redisCommand;
        this.keyColumn = keyColumn;
        this.valueColumn = valueColumn;
        this.putIfAbsent = putIfAbsent;
    }

    public RowRedisMapper(RedisCommand redisCommand,  String keyColumn, String fieldColumn, String valueColumn, boolean putIfAbsent, int ttl){
        this.ttl = ttl;
        this.redisCommand = redisCommand;
        this.keyColumn = keyColumn;
        this.fieldColumn = fieldColumn;
        this.valueColumn = valueColumn;
        this.putIfAbsent = putIfAbsent;
    }

    public RowRedisMapper(RedisCommand redisCommand){
        this.redisCommand = redisCommand;
    }

    public RowRedisMapper(RedisCommand redisCommand, Map<String, String> config){
        this.redisCommand = redisCommand;
    }

    public RowRedisMapper(RedisCommand redisCommand, ReadableConfig config){
        this.redisCommand = redisCommand;
        this.ttl = config.get(RedisOptions.TTL);
        this.valueColumn = config.get(RedisOptions.VALUE_COLUMN);
        this.keyColumn = config.get(RedisOptions.KEY_COLUMN);
        this.fieldColumn = config.get(RedisOptions.FIELD_COLUMN);
        this.putIfAbsent = config.get(RedisOptions.PUT_IF_ABSENT);
    }

    @Override
    public RedisCommandDescription getCommandDescription() {
        return new RedisCommandDescription(redisCommand, ttl, keyColumn, fieldColumn, valueColumn, putIfAbsent);
    }

    @Override
    public String getKeyFromData(GenericRowData row, Integer keyIndex) {
        return String.valueOf(row.getField(keyIndex));
    }

    @Override
    public String getValueFromData(GenericRowData row,  Integer valueIndex) {
        return String.valueOf(row.getField(valueIndex));
    }

    @Override
    public String getFieldFromData(GenericRowData row, Integer fieldIndex) {
        return String.valueOf(row.getField(fieldIndex));
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
        RedisCommand redisCommand = ((RowRedisMapper) obj).redisCommand;
        return this.redisCommand == redisCommand;
    }

}
