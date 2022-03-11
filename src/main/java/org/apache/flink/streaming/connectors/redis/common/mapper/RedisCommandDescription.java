package org.apache.flink.streaming.connectors.redis.common.mapper;

import java.io.Serializable;

/**
 *
 */
public class RedisCommandDescription extends RedisCommandBaseDescription implements Serializable {

    private static final long serialVersionUID = 1L;

    private Integer ttl;

    public RedisCommandDescription(RedisCommand redisCommand, Integer ttl) {
        super(redisCommand);

        this.ttl = ttl;
    }

    public Integer getTTL() {
        return ttl;
    }
}
