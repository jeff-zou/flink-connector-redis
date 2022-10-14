package org.apache.flink.streaming.connectors.redis.common.mapper.row.lookup;

import org.apache.flink.streaming.connectors.redis.common.hanlder.RedisMapperHandler;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandBaseDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.streaming.connectors.redis.descriptor.RedisValidator.REDIS_COMMAND;

/** row redis mapper. @Author: jeff.zou @Date: 2022/3/7.14:59 */
public class RowRedisMapper<OUT> implements RedisMapper<OUT>, RedisMapperHandler {

    RedisCommand redisCommand;

    public RowRedisMapper(RedisCommand redisCommand) {
        this.redisCommand = redisCommand;
    }

    @Override
    public Map<String, String> requiredContext() {
        Map<String, String> require = new HashMap<>();
        require.put(REDIS_COMMAND, getRedisCommand().name());
        return require;
    }

    @Override
    public List<String> supportProperties() throws Exception {
        return null;
    }

    public RedisCommand getRedisCommand() {
        return redisCommand;
    }

    @Override
    public RedisCommandBaseDescription getCommandDescription() {
        return new RedisCommandBaseDescription(redisCommand);
    }
}
