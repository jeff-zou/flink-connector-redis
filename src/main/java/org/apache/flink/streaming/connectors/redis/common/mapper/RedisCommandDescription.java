package org.apache.flink.streaming.connectors.redis.common.mapper;

import java.io.Serializable;
import java.util.Objects;

public class RedisCommandDescription extends RedisCommandBaseDescription implements Serializable {

    private static final long serialVersionUID = 1L;

    private boolean putIfAbsent;

    private Integer ttl;

    public RedisCommandDescription(RedisCommand redisCommand, Integer ttl, boolean putIfAbsent) {
        super(redisCommand);

        this.ttl = ttl;
        this.putIfAbsent = putIfAbsent;

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

    public boolean isPutIfAbsent() {
        return putIfAbsent;
    }

    public Integer getTTL() {
        return ttl;
    }
}
