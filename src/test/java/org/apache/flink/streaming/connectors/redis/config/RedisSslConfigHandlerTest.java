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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.connectors.redis.table.RedisDynamicTableFactory;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests that username, ssl and ssl.verify-peer reach the {@link FlinkConfigBase} of every redis
 * mode. Does not require a running redis instance.
 */
public class RedisSslConfigHandlerTest {

    private static Configuration singleOptions() {
        Configuration configuration = new Configuration();
        configuration.setString(RedisOptions.HOST.key(), "localhost");
        return configuration;
    }

    private static Configuration clusterOptions() {
        Configuration configuration = new Configuration();
        configuration.setString(RedisOptions.CLUSTERNODES.key(), "localhost:7000,localhost:7001");
        return configuration;
    }

    private static Configuration sentinelOptions() {
        Configuration configuration = new Configuration();
        configuration.setString(RedisOptions.REDIS_MASTER_NAME.key(), "master");
        configuration.setString(RedisOptions.SENTINELS_INFO.key(), "localhost:26379");
        return configuration;
    }

    private static void withSslOptions(Configuration configuration) {
        configuration.setString(RedisOptions.USERNAME.key(), "flink");
        configuration.setString(RedisOptions.PASSWORD.key(), "pwd");
        configuration.setString(RedisOptions.SSL.key(), "true");
        configuration.setString(RedisOptions.SSL_VERIFY_PEER.key(), "false");
    }

    private static void assertSslOptionsApplied(FlinkConfigBase config) {
        assertEquals("flink", config.getUsername());
        assertEquals("pwd", config.getPassword());
        assertTrue(config.isSsl());
        assertFalse(config.isSslVerifyPeer());
    }

    private static void assertDefaults(FlinkConfigBase config) {
        assertNull(config.getUsername());
        assertFalse(config.isSsl());
        assertTrue(config.isSslVerifyPeer());
    }

    @Test
    public void testSingleConfigHandler() {
        Configuration configuration = singleOptions();
        withSslOptions(configuration);

        assertSslOptionsApplied(new FlinkSingleConfigHandler().createFlinkConfig(configuration));
    }

    @Test
    public void testClusterConfigHandler() {
        Configuration configuration = clusterOptions();
        withSslOptions(configuration);

        assertSslOptionsApplied(new FlinkClusterConfigHandler().createFlinkConfig(configuration));
    }

    @Test
    public void testSentinelConfigHandler() {
        Configuration configuration = sentinelOptions();
        withSslOptions(configuration);

        assertSslOptionsApplied(new FlinkSentinelConfigHandler().createFlinkConfig(configuration));
    }

    @Test
    public void testDefaultsAreBackwardsCompatible() {
        assertDefaults(new FlinkSingleConfigHandler().createFlinkConfig(singleOptions()));
        assertDefaults(new FlinkClusterConfigHandler().createFlinkConfig(clusterOptions()));
        assertDefaults(new FlinkSentinelConfigHandler().createFlinkConfig(sentinelOptions()));
    }

    @Test
    public void testOptionsAreDeclaredByTheFactory() {
        assertTrue(
                new RedisDynamicTableFactory()
                        .optionalOptions()
                        .containsAll(
                                Arrays.asList(
                                        RedisOptions.USERNAME,
                                        RedisOptions.SSL,
                                        RedisOptions.SSL_VERIFY_PEER)));
    }
}
