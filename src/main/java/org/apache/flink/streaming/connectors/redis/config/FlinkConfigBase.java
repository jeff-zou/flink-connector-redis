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

import org.apache.flink.util.Preconditions;

import java.io.Serializable;

/** Base class for Flink Redis configuration. */
public abstract class FlinkConfigBase implements Serializable {

    private static final long serialVersionUID = 1L;

    protected final int connectionTimeout;

    protected final String password;

    protected final Boolean ssl;

    protected final LettuceConfig lettuceConfig;

    protected FlinkConfigBase(int connectionTimeout, String password, Boolean ssl, LettuceConfig lettuceConfig) {
        Preconditions.checkArgument(
                connectionTimeout >= 0, "connection timeout can not be negative");
        this.password = password;
        this.connectionTimeout = connectionTimeout;
        this.lettuceConfig = lettuceConfig;
        this.ssl = ssl;
    }

    public String getPassword() {
        return password;
    }

    /**
     * Returns timeout.
     *
     * @return connection timeout
     */
    public int getConnectionTimeout() {
        return connectionTimeout;
    }

    /**
     * Returns ssl.
     *
     * @return ssl
     */
    public Boolean getSsl() {
        return ssl;
    }

    public LettuceConfig getLettuceConfig() {
        return lettuceConfig;
    }

    @Override
    public String toString() {
        return "FlinkConfigBase{"
                + "connectionTimeout="
                + connectionTimeout
                + ", password='"
                + password
                + '\''
                + ", lettuceConfig="
                + lettuceConfig
                + '}';
    }
}
