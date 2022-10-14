package org.apache.flink.streaming.connectors.redis.common.config;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.util.Preconditions;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.streaming.connectors.redis.descriptor.RedisValidator.REDIS_CLUSTER;
import static org.apache.flink.streaming.connectors.redis.descriptor.RedisValidator.REDIS_MODE;

/** cluster config handler to find and create cluster config use meta. */
public class FlinkClusterConfigHandler implements FlinkConfigHandler {

    @Override
    public FlinkConfigBase createFlinkConfig(ReadableConfig config) {
        Preconditions.checkState(
                config.get(RedisOptions.DATABASE) == 0, "redis cluster just support db 0");
        String nodesInfo = config.get(RedisOptions.CLUSTERNODES);
        Preconditions.checkNotNull(nodesInfo, "nodes should not be null in cluster mode");

        FlinkClusterConfig.Builder builder =
                new FlinkClusterConfig.Builder()
                        .setNodesInfo(nodesInfo)
                        .setPassword(config.get(RedisOptions.PASSWORD));

        builder.setTimeout(config.get(RedisOptions.TIMEOUT));

        return builder.build();
    }

    @Override
    public Map<String, String> requiredContext() {
        Map<String, String> require = new HashMap<>();
        require.put(REDIS_MODE, REDIS_CLUSTER);
        return require;
    }

    public FlinkClusterConfigHandler() {}
}
