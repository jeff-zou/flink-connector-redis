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
    private FlinkClusterConfig(
            String nodesInfo, int connectionTimeout, String password, Boolean ssl, LettuceConfig lettuceConfig) {
        super(connectionTimeout, password, ssl, lettuceConfig);

        Objects.requireNonNull(nodesInfo, "nodesInfo information should be presented");
        this.nodesInfo = nodesInfo;
    }

    /** Builder for initializing {@link FlinkClusterConfig}. */
    public static class Builder {

        private String nodesInfo;
        private int timeout;
        private String password;
        private Boolean ssl;

        private LettuceConfig lettuceConfig;

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
         * Builds ClusterConfig.
         *
         * @return ClusterConfig
         */
        public FlinkClusterConfig build() {
            return new FlinkClusterConfig(nodesInfo, timeout, password, ssl, lettuceConfig);
        }
    }
}
