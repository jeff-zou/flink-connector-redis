package org.apache.flink.streaming.connectors.redis.common.mapper.row.sink;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;

/**
 * @Author: jeff.zou @Date: 2022/3/22.11:11
 */
public class HDelSinkMapper extends RowRedisSinkMapper {
    public HDelSinkMapper() {
        super(RedisCommand.HDEL);
    }

    public HDelSinkMapper(ReadableConfig readableConfig) {
        super(RedisCommand.HDEL, readableConfig);
    }
}
