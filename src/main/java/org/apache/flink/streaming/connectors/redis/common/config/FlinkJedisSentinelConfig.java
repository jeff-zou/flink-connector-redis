package org.apache.flink.streaming.connectors.redis.common.config;

import org.apache.flink.streaming.connectors.redis.common.util.CheckUtil;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Protocol;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/** Configuration for Jedis Sentinel pool. */
public class FlinkJedisSentinelConfig extends FlinkJedisConfigBase {
    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(FlinkJedisSentinelConfig.class);

    private final String masterName;
    private final String sentinelsInfo;
    private final int soTimeout;
    private final int database;

    private final String sentinelsPassword;

    /**
     * Jedis Sentinels config. The master name and sentinels are mandatory, and when you didn't set
     * these, it throws NullPointerException.
     *
     * @param masterName master name of the replica set
     * @param sentinelsInfo set of sentinel hosts
     * @param connectionTimeout timeout connection timeout
     * @param soTimeout timeout socket timeout
     * @param password password, if any
     * @param database database database index
     * @param maxTotal maxTotal the maximum number of objects that can be allocated by the pool
     * @param maxIdle the cap on the number of "idle" instances in the pool
     * @param minIdle the minimum number of idle objects to maintain in the pool
     * @throws NullPointerException if {@code masterName} or {@code sentinels} is {@code null}
     * @throws IllegalArgumentException if {@code sentinels} are empty
     */
    private FlinkJedisSentinelConfig(
            String masterName,
            String sentinelsInfo,
            int connectionTimeout,
            int soTimeout,
            String password,
            int database,
            int maxTotal,
            int maxIdle,
            int minIdle,
            String sentinelsPassword) {
        super(connectionTimeout, maxTotal, maxIdle, minIdle, password);
        Objects.requireNonNull(masterName, "Master name should be presented");
        Objects.requireNonNull(sentinelsInfo, "Sentinels information should be presented");
        this.sentinelsPassword = sentinelsPassword;
        this.masterName = masterName;
        this.sentinelsInfo = sentinelsInfo;
        this.soTimeout = soTimeout;
        this.database = database;
    }

    /**
     * Returns master name of the replica set.
     *
     * @return master name of the replica set.
     */
    public String getMasterName() {
        return masterName;
    }

    /**
     * Returns Sentinels host addresses.
     *
     * @return Set of Sentinels host addresses
     */
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

    /** Builder for initializing {@link FlinkJedisSentinelConfig}. */
    public static class Builder {
        private String masterName;
        private String sentinelsInfo;
        private int connectionTimeout = Protocol.DEFAULT_TIMEOUT;
        private int soTimeout = Protocol.DEFAULT_TIMEOUT;
        private String password;
        private int database = Protocol.DEFAULT_DATABASE;
        private int maxTotal = GenericObjectPoolConfig.DEFAULT_MAX_TOTAL;
        private int maxIdle = GenericObjectPoolConfig.DEFAULT_MAX_IDLE;
        private int minIdle = GenericObjectPoolConfig.DEFAULT_MIN_IDLE;

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

        /**
         * Sets sentinels address.
         *
         * @param   sentinelsInfo
         * @return Builder itself
         */
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
         * Sets value for the {@code maxTotal} configuration attribute for pools to be created with
         * this configuration instance.
         *
         * @param maxTotal maxTotal the maximum number of objects that can be allocated by the pool,
         *     default value is 8
         * @return Builder itself
         */
        public Builder setMaxTotal(int maxTotal) {
            this.maxTotal = maxTotal;
            return this;
        }

        /**
         * Sets value for the {@code maxIdle} configuration attribute for pools to be created with
         * this configuration instance.
         *
         * @param maxIdle the cap on the number of "idle" instances in the pool, default value is 8
         * @return Builder itself
         */
        public Builder setMaxIdle(int maxIdle) {
            this.maxIdle = maxIdle;
            return this;
        }

        /**
         * Sets value for the {@code minIdle} configuration attribute for pools to be created with
         * this configuration instance.
         *
         * @param minIdle the minimum number of idle objects to maintain in the pool, default value
         *     is 0
         * @return Builder itself
         */
        public Builder setMinIdle(int minIdle) {
            this.minIdle = minIdle;
            return this;
        }

        public Builder setSentinelsPassword(String sentinelsPassword) {
            this.sentinelsPassword = sentinelsPassword;
            return this;
        }

        /**
         * Builds JedisSentinelConfig.
         *
         * @return JedisSentinelConfig
         */
        public FlinkJedisSentinelConfig build() {
            return new FlinkJedisSentinelConfig(
                    masterName,
                    sentinelsInfo,
                    connectionTimeout,
                    soTimeout,
                    password,
                    database,
                    maxTotal,
                    maxIdle,
                    minIdle,
                    sentinelsPassword);
        }
    }

    @Override
    public String toString() {
        return "FlinkJedisSentinelConfig{" +
                "masterName='" + masterName + '\'' +
                ", sentinelsInfo=" + sentinelsInfo +
                ", soTimeout=" + soTimeout +
                ", database=" + database +
                ", sentinelsPassword='" + sentinelsPassword + '\'' +
                '}';
    }
}
