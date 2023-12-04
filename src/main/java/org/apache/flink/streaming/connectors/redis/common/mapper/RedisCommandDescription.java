package org.apache.flink.streaming.connectors.redis.common.mapper;

import java.io.Serializable;
import java.time.LocalTime;

/** */
public class RedisCommandDescription extends RedisCommandBaseDescription implements Serializable {

    private static final long serialVersionUID = 1L;

    private Integer ttl;

    private Boolean setIfAbsent;

    private LocalTime expireTime;

    private boolean ttlKeyNotAbsent;

    public RedisCommandDescription(
            RedisCommand redisCommand,
            Integer ttl,
            LocalTime expireTime,
            Boolean setIfAbsent,
            Boolean ttlKeyNotAbsent) {
        super(redisCommand);
        this.expireTime = expireTime;
        this.ttl = ttl;
        this.setIfAbsent = setIfAbsent;
        this.ttlKeyNotAbsent = ttlKeyNotAbsent;
    }

    public Integer getTTL() {
        return ttl;
    }

    public LocalTime getExpireTime() {
        return expireTime;
    }

    public Boolean getSetIfAbsent() {
        return setIfAbsent;
    }

    public boolean getTtlKeyNotAbsent() {
        return ttlKeyNotAbsent;
    }
}
