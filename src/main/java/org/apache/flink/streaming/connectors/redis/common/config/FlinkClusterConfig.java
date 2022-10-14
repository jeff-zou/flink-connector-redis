package org.apache.flink.streaming.connectors.redis.common.config;

import java.util.Objects;

/** Configuration for cluster. */
public class FlinkClusterConfig extends FlinkConfigBase {
    private static final long serialVersionUID = 1L;

    private final String nodesInfo;

    public String getNodesInfo() {
        return nodesInfo;
    }

    /**
     * cluster configuration. The list of node is mandatory, and when nodes is not set, it throws
     * NullPointerException.
     *
     * @param nodesInfo list of node information for Cluster
     * @param connectionTimeout socket / connection timeout. The default is 2000
     * @param password limit of redirections-how much we'll follow MOVED or ASK
     * @throws NullPointerException if parameter {@code nodes} is {@code null}
     */
    private FlinkClusterConfig(String nodesInfo, int connectionTimeout, String password) {
        super(connectionTimeout, password);

        Objects.requireNonNull(nodesInfo, "nodesInfo information should be presented");
        this.nodesInfo = nodesInfo;
    }

    /** Builder for initializing {@link FlinkClusterConfig}. */
    public static class Builder {
        private String nodesInfo;
        private int timeout;
        private String password;

        public Builder setNodesInfo(String nodesInfo) {
            this.nodesInfo = nodesInfo;
            return this;
        }

        /**
         * Sets socket / connection timeout.
         *
         * @param timeout socket / connection timeout, default value is 2000
         * @return Builder itself
         */
        public Builder setTimeout(int timeout) {
            this.timeout = timeout;
            return this;
        }

        public Builder setPassword(String password) {
            this.password = password;
            return this;
        }

        /**
         * Builds ClusterConfig.
         *
         * @return ClusterConfig
         */
        public FlinkClusterConfig build() {
            return new FlinkClusterConfig(nodesInfo, timeout, password);
        }
    }

    @Override
    public String toString() {
        return "FlinkClusterConfig{"
                + "connectionTimeout="
                + connectionTimeout
                + ", password='"
                + password
                + '\''
                + '}';
    }
}
