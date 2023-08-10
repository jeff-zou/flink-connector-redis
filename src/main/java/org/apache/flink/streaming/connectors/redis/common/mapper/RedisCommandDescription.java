package org.apache.flink.streaming.connectors.redis.common.mapper;

import java.io.Serializable;
import java.time.LocalTime;

/** */
public class RedisCommandDescription extends RedisCommandBaseDescription implements Serializable {

    private static final long serialVersionUID = 1L;

    private Integer ttl;

    private Boolean setIfAbsent;

    private LocalTime expireTime;

    public RedisCommandDescription(
            RedisCommand redisCommand, Integer ttl, LocalTime expireTime, Boolean setIfAbsent) {
        super(redisCommand);
        this.expireTime = expireTime;
        this.ttl = ttl;
        this.setIfAbsent = setIfAbsent;
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
}
