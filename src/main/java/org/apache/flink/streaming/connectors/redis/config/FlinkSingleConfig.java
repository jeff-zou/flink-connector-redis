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
            String host,
            int port,
            int connectionTimeout,
            String password,
            int database,
            Boolean ssl,
            LettuceConfig lettuceConfig) {
        super(connectionTimeout, password, ssl, lettuceConfig);
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

    /**
     * Returns ssl.
     *
     * @return ssl
     */
    public Boolean getSsl() {
        return ssl;
    }

    /** Builder for initializing {@link FlinkSingleConfig}. */
    public static class Builder {

        private String host;
        private int port;
        private int timeout;
        private int database;
        private String password;
        private Boolean ssl;

        private LettuceConfig lettuceConfig;

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
         * Sets ssl.
         *
         * @param ssl whether enable ssl
         * @return Builder itself
         */
        public Builder setSsl(Boolean ssl) {
            this.ssl = ssl;
            return this;
        }

        public Builder setLettuceConfig(LettuceConfig lettuceConfig) {
            this.lettuceConfig = lettuceConfig;
            return this;
        }

        /**
         * Builds PoolConfig.
         *
         * @return PoolConfig
         */
        public FlinkSingleConfig build() {
            return new FlinkSingleConfig(host, port, timeout, password, database, ssl, lettuceConfig);
        }
    }
}
