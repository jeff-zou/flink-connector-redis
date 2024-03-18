package org.apache.flink.streaming.connectors.redis.config;

import static org.apache.flink.streaming.connectors.redis.config.RedisValidator.REDIS_MODE;
import static org.apache.flink.streaming.connectors.redis.config.RedisValidator.REDIS_SENTINEL;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import java.util.HashMap;
import java.util.Map;

/** */
public class FlinkSentinelConfigHandler implements FlinkConfigHandler {

    @Override
    public FlinkConfigBase createFlinkConfig(ReadableConfig config) {
        String masterName = config.get(RedisOptions.REDIS_MASTER_NAME);
        String sentinelsInfo = config.get(RedisOptions.SENTINELS_INFO);
        String sentinelsPassword =
                StringUtils.isNullOrWhitespaceOnly(config.get(RedisOptions.SENTINELS_PASSWORD))
                        ? null
                        : config.get(RedisOptions.SENTINELS_PASSWORD);
        Preconditions.checkNotNull(masterName, "master should not be null in sentinel mode");
        Preconditions.checkNotNull(sentinelsInfo, "sentinels should not be null in sentinel mode");

        LettuceConfig lettuceConfig =
                new LettuceConfig(
                        config.get(RedisOptions.NETTY_IO_POOL_SIZE),
                        config.get(RedisOptions.NETTY_EVENT_POOL_SIZE));

        FlinkSentinelConfig flinkSentinelConfig =
                new FlinkSentinelConfig.Builder()
                        .setSentinelsInfo(sentinelsInfo)
                        .setMasterName(masterName)
                        .setConnectionTimeout(config.get(RedisOptions.TIMEOUT))
                        .setDatabase(config.get(RedisOptions.DATABASE))
                        .setPassword(config.get(RedisOptions.PASSWORD))
                        .setSentinelsPassword(sentinelsPassword)
                        .setLettuceConfig(lettuceConfig)
                        .build();
        return flinkSentinelConfig;
    }

    @Override
    public Map<String, String> requiredContext() {
        Map<String, String> require = new HashMap<>();
        require.put(REDIS_MODE, REDIS_SENTINEL);
        return require;
    }

    public FlinkSentinelConfigHandler() {}
}
