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

package org.apache.flink.streaming.connectors.redis.container;

import org.apache.flink.streaming.connectors.redis.config.FlinkSingleConfig;
import org.junit.jupiter.api.Test;

import io.lettuce.core.RedisURI;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests that the authentication and the TLS/SSL settings end up on the {@link RedisURI} handed to
 * lettuce. Does not require a running redis instance.
 */
public class RedisClientBuilderSslTest {

    private static RedisURI uriFor(FlinkSingleConfig config) {
        RedisURI.Builder builder = RedisURI.builder().withHost("localhost").withPort(6379);
        RedisClientBuilder.applyAuthenticationAndSsl(builder, config);
        return builder.build();
    }

    private static FlinkSingleConfig.Builder configBuilder() {
        return new FlinkSingleConfig.Builder().setHost("localhost").setPort(6379);
    }

    @Test
    public void testPasswordOnlyKeepsUsernameUnset() {
        RedisURI uri = uriFor(configBuilder().setPassword("pwd").build());

        assertNull(uri.getUsername());
        assertArrayEquals("pwd".toCharArray(), uri.getPassword());
        assertFalse(uri.isSsl());
    }

    @Test
    public void testUsernameAndPasswordUseAclAuthentication() {
        RedisURI uri = uriFor(configBuilder().setUsername("flink").setPassword("pwd").build());

        assertEquals("flink", uri.getUsername());
        assertArrayEquals("pwd".toCharArray(), uri.getPassword());
    }

    @Test
    public void testUsernameWithoutPasswordIsIgnored() {
        RedisURI uri = uriFor(configBuilder().setUsername("flink").build());

        assertNull(uri.getUsername());
        assertNull(uri.getPassword());
    }

    @Test
    public void testBlankPasswordDoesNotAuthenticate() {
        RedisURI uri = uriFor(configBuilder().setUsername("flink").setPassword("  ").build());

        assertNull(uri.getUsername());
        assertNull(uri.getPassword());
    }

    @Test
    public void testSslDisabledByDefault() {
        RedisURI uri = uriFor(configBuilder().build());

        assertFalse(uri.isSsl());
    }

    @Test
    public void testSslVerifiesPeerByDefault() {
        RedisURI uri = uriFor(configBuilder().setSsl(true).build());

        assertTrue(uri.isSsl());
        assertTrue(uri.isVerifyPeer());
    }

    @Test
    public void testSslVerifyPeerCanBeDisabled() {
        RedisURI uri = uriFor(configBuilder().setSsl(true).setSslVerifyPeer(false).build());

        assertTrue(uri.isSsl());
        assertFalse(uri.isVerifyPeer());
    }

    @Test
    public void testSslVerifyPeerIsIgnoredWhenSslIsDisabled() {
        RedisURI uri = uriFor(configBuilder().setSsl(false).setSslVerifyPeer(false).build());

        assertFalse(uri.isSsl());
        assertTrue(uri.isVerifyPeer());
    }
}
