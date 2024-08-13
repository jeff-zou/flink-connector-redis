/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.redis.config;

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
    private final Boolean ssl;

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
            String sentinelsPassword,
            Boolean ssl,
            LettuceConfig lettuceConfig) {
        super(connectionTimeout, password, ssl, lettuceConfig);
        Objects.requireNonNull(masterName, "Master name should be presented");
        Objects.requireNonNull(sentinelsInfo, "Sentinels information should be presented");
        this.masterName = masterName;
        this.sentinelsInfo = sentinelsInfo;
        this.soTimeout = soTimeout;
        this.database = database;
        this.sentinelsPassword = sentinelsPassword;
        this.ssl = ssl;
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
     * Returns ssl.
     *
     * @return ssl
     */
    public Boolean getSsl() {
        return ssl;
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
        private Boolean ssl;

        private LettuceConfig lettuceConfig;

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

        /**
         * Sets ssl.
         *
         * @param ssl whether enable ssl
         * @return Builder itself
         */
        public Builder setSsl(Boolean ssl) {
            this.ssl = ssl;
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

        public Builder setLettuceConfig(LettuceConfig lettuceConfig) {
            this.lettuceConfig = lettuceConfig;
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
                    sentinelsPassword,
                    ssl,
                    lettuceConfig);
        }
    }
}
