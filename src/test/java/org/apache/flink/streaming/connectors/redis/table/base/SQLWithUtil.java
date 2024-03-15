package org.apache.flink.streaming.connectors.redis.table.base;

import static org.apache.flink.streaming.connectors.redis.table.base.TestRedisConfigBase.CLUSTERNODES;
import static org.apache.flink.streaming.connectors.redis.table.base.TestRedisConfigBase.CLUSTER_PASSWORD;
import static org.apache.flink.streaming.connectors.redis.table.base.TestRedisConfigBase.REDIS_HOST;
import static org.apache.flink.streaming.connectors.redis.table.base.TestRedisConfigBase.REDIS_PASSWORD;
import static org.apache.flink.streaming.connectors.redis.table.base.TestRedisConfigBase.REDIS_PORT;

public class SQLWithUtil {
    public static String sigleWith() {
        return "'connector'='redis', "
                + "'host'='"
                + REDIS_HOST
                + "','port'='"
                + REDIS_PORT
                + "', 'redis-mode'='single','password'='"
                + REDIS_PASSWORD
                + "',";
    }

    public static String clusterWith() {
        return "'connector'='redis', "
                + "'cluster-nodes'='"
                + CLUSTERNODES
                + "','redis-mode'='cluster',"
                + " 'password'='"
                + CLUSTER_PASSWORD
                + "',";
    }
}
