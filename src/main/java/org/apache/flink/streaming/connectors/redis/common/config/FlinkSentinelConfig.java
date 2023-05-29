package org.apache.flink.streaming.connectors.redis.common.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/** Configuration for Sentinel pool. */
public class FlinkSentinelConfig extends FlinkConfigBase {
    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(FlinkSentinelConfig.class);

    private final String sentinelsInfo;
    private final int soTimeout;
    private final int database;
    private final String masterName;

    private final String sentinelsPassword;

    /**
     * Sentinels config. The master name and sentinels are mandatory, and when you didn't set these,
     * it throws NullPointerException.
     *
     * @param masterName master name of the replica set
     * @param sentinelsInfo set of sentinel hosts
     * @param connectionTimeout timeout connection timeout
     * @param soTimeout timeout socket timeout
     * @param database database database index
     * @throws NullPointerException if {@code masterName} or {@code sentinels} is {@code null}
     * @throws IllegalArgumentException if {@code sentinels} are empty
     */
    private FlinkSentinelConfig(
            String masterName,
            String sentinelsInfo,
            int connectionTimeout,
            int soTimeout,
            int database,
            String password,
            String sentinelsPassword) {
        super(connectionTimeout, password);
        Objects.requireNonNull(masterName, "Master name should be presented");
        Objects.requireNonNull(sentinelsInfo, "Sentinels information should be presented");
        this.masterName = masterName;
        this.sentinelsInfo = sentinelsInfo;
        this.soTimeout = soTimeout;
        this.database = database;
        this.sentinelsPassword = sentinelsPassword;
    }

    /**
     * Returns master name of the replica set.
     *
     * @return master name of the replica set.
     */
    public String getMasterName() {
        return masterName;
    }

    public String getSentinelsInfo() {
        return sentinelsInfo;
    }

    /**
     * Returns socket timeout.
     *
     * @return socket timeout
     */
    public int getSoTimeout() {
        return soTimeout;
    }

    /**
     * Returns database index.
     *
     * @return database index
     */
    public int getDatabase() {
        return database;
    }

    public String getSentinelsPassword() {
        return sentinelsPassword;
    }

    /** Builder for initializing {@link FlinkSentinelConfig}. */
    public static class Builder {
        private String masterName;
        private String sentinelsInfo;
        private int connectionTimeout;
        private int soTimeout;
        private int database;
        private String password;
        private String sentinelsPassword;

        /**
         * Sets master name of the replica set.
         *
         * @param masterName master name of the replica set
         * @return Builder itself
         */
        public Builder setMasterName(String masterName) {
            this.masterName = masterName;
            return this;
        }

        public Builder setSentinelsInfo(String sentinelsInfo) {
            this.sentinelsInfo = sentinelsInfo;
            return this;
        }

        /**
         * Sets connection timeout.
         *
         * @param connectionTimeout connection timeout, default value is 2000
         * @return Builder itself
         */
        public Builder setConnectionTimeout(int connectionTimeout) {
            this.connectionTimeout = connectionTimeout;
            return this;
        }

        /**
         * Sets socket timeout.
         *
         * @param soTimeout socket timeout, default value is 2000
         * @return Builder itself
         */
        public Builder setSoTimeout(int soTimeout) {
            this.soTimeout = soTimeout;
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

        public Builder setPassword(String password) {
            this.password = password;
            return this;
        }

        public Builder setSentinelsPassword(String sentinelsPassword) {
            this.sentinelsPassword = sentinelsPassword;
            return this;
        }

        /**
         * Builds SentinelConfig.
         *
         * @return SentinelConfig
         */
        public FlinkSentinelConfig build() {
            return new FlinkSentinelConfig(
                    masterName,
                    sentinelsInfo,
                    connectionTimeout,
                    soTimeout,
                    database,
                    password,
                    sentinelsPassword);
        }
    }

    @Override
    public String toString() {
        return "FlinkSentinelConfig{"
                + "sentinelsInfo='"
                + sentinelsInfo
                + '\''
                + ", soTimeout="
                + soTimeout
                + ", database="
                + database
                + ", masterName='"
                + masterName
                + '\''
                + ", sentinelsPassword='"
                + sentinelsPassword
                + '\''
                + ", connectionTimeout="
                + connectionTimeout
                + ", password='"
                + password
                + '\''
                + '}';
    }
}
