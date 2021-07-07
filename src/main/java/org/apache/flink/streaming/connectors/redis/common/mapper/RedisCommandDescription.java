package org.apache.flink.streaming.connectors.redis.common.mapper;

import java.io.Serializable;
import java.util.Objects;

public class RedisCommandDescription implements Serializable {

    private static final long serialVersionUID = 1L;

    private RedisCommand redisCommand;

    private String keyColumn;

    //the score of sorted set or the field of hash
    private String fieldColumn;

    private String valueColumn;

    private boolean putIfAbsent;

    private Integer ttl;

    public RedisCommandDescription(RedisCommand redisCommand, Integer ttl, String keyColumn, String fieldColumn, String valueColumn, boolean putIfAbsent) {
        Objects.requireNonNull(redisCommand, "Redis command type can not be null");
        Objects.requireNonNull(keyColumn, "Redis key-column can not be null");
        this.redisCommand = redisCommand;
        this.ttl = ttl;
        this.keyColumn = keyColumn;
        this.fieldColumn = fieldColumn;
        this.valueColumn = valueColumn;
        this.putIfAbsent = putIfAbsent;

        if (redisCommand.getRedisDataType() == RedisDataType.HASH ) {
            if (fieldColumn == null) {
                throw new IllegalArgumentException("Hash should have field column");
            }
        }

        if (redisCommand.getRedisDataType() == RedisDataType.SORTED_SET) {
            if (fieldColumn == null) {
                throw new IllegalArgumentException("Sorted Set should have score column which named by 'field-column'");
            }
        }

        if (redisCommand.equals(RedisCommand.SETEX)) {
            if (ttl == null) {
                throw new IllegalArgumentException("SETEX command should have time to live (TTL)");
            }
        }

        if (redisCommand.equals(RedisCommand.INCRBY_EX)) {
            if (ttl == null) {
                throw new IllegalArgumentException("INCRBY_EX command should have time to live (TTL)");
            }
        }

        if (redisCommand.equals(RedisCommand.DESCRBY_EX)) {
            if (ttl == null) {
                throw new IllegalArgumentException("INCRBY_EX command should have time to live (TTL)");
            }
        }
    }

    /**
     * Returns the {@link RedisCommand}.
     *
     * @return the command type of the mapping
     */
    public RedisCommand getCommand() {
        return redisCommand;
    }

    public String getKeyColumn() {
        return keyColumn;
    }

    public String getFieldColumn() {
        return fieldColumn;
    }

    public String getValueColumn() {
        return valueColumn;
    }

    public boolean isPutIfAbsent() {
        return putIfAbsent;
    }

    public Integer getTTL() {
        return ttl;
    }
}
