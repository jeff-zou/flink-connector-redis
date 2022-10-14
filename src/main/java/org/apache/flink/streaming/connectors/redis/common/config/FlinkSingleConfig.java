package org.apache.flink.streaming.connectors.redis.common.config;

import java.util.Objects;

/** Configuration for pool. */
public class FlinkSingleConfig extends FlinkConfigBase {

    private static final long serialVersionUID = 1L;

    private final String host;
    private final int port;
    private final int database;

    /**
     * pool configuration. The host is mandatory, and when host is not set, it throws
     * NullPointerException.
     *
     * @param host hostname or IP
     * @param port port, default value is 6379
     * @param connectionTimeout socket / connection timeout, default value is 2000 milli second
     * @param database database index
     * @throws NullPointerException if parameter {@code host} is {@code null}
     */
    private FlinkSingleConfig(
            String host, int port, int connectionTimeout, String password, int database) {
        super(connectionTimeout, password);
        Objects.requireNonNull(host, "Host information should be presented");
        this.host = host;
        this.port = port;
        this.database = database;
    }

    /**
     * Returns host.
     *
     * @return hostname or IP
     */
    public String getHost() {
        return host;
    }

    /**
     * Returns port.
     *
     * @return port
     */
    public int getPort() {
        return port;
    }

    /**
     * Returns database index.
     *
     * @return database index
     */
    public int getDatabase() {
        return database;
    }

    /** Builder for initializing {@link FlinkSingleConfig}. */
    public static class Builder {
        private String host;
        private int port;
        private int timeout;
        private int database;
        private String password;

        /**
         * Sets host.
         *
         * @param host host
         * @return Builder itself
         */
        public Builder setHost(String host) {
            this.host = host;
            return this;
        }

        /**
         * Sets port.
         *
         * @param port port, default value is 6379
         * @return Builder itself
         */
        public Builder setPort(int port) {
            this.port = port;
            return this;
        }

        /**
         * Sets timeout.
         *
         * @param timeout timeout, default value is 2000
         * @return Builder itself
         */
        public Builder setTimeout(int timeout) {
            this.timeout = timeout;
            return this;
        }

        /**
         * Sets database index.
         *
         * @param database database index, default value is 0
         * @return Builder itself
         */
        public Builder setDatabase(int database) {
            this.database = database;
            return this;
        }

        /**
         * Sets password.
         *
         * @param password password, if any
         * @return Builder itself
         */
        public Builder setPassword(String password) {
            this.password = password;
            return this;
        }

        /**
         * Builds PoolConfig.
         *
         * @return PoolConfig
         */
        public FlinkSingleConfig build() {
            return new FlinkSingleConfig(host, port, timeout, password, database);
        }
    }

    @Override
    public String toString() {
        return "PoolConfig{"
                + "host='"
                + host
                + '\''
                + ", port="
                + port
                + ", timeout="
                + connectionTimeout
                + ", database="
                + database
                + '}';
    }
}
