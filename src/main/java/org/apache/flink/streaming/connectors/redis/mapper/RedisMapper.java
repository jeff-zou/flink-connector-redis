package org.apache.flink.streaming.connectors.redis.mapper;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.streaming.connectors.redis.command.RedisCommandBaseDescription;

import java.io.Serializable;

/**
 * @param <T>
 */
public interface RedisMapper<T> extends Function, Serializable {

    /**
     * Returns descriptor which defines data type.
     *
     * @return data type descriptor
     */
    RedisCommandBaseDescription getCommandDescription();
}
