package org.apache.flink.streaming.connectors.redis.common.mapper.row.sink;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;

/** @Author: jeff.zou @Date: 2022/3/23.11:11 */
public class ZRemSinkMapper extends RowRedisSinkMapper {
    public ZRemSinkMapper() {
        super(RedisCommand.ZREM);
    }

    public ZRemSinkMapper(ReadableConfig readableConfig) {
        super(RedisCommand.ZREM, readableConfig);
    }
}
