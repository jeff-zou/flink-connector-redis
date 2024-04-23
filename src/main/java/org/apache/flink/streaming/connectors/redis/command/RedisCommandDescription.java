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

package org.apache.flink.streaming.connectors.redis.command;

import java.io.Serializable;
import java.time.LocalTime;

/** */
public class RedisCommandDescription extends RedisCommandBaseDescription implements Serializable {

    private static final long serialVersionUID = 1L;

    private Integer ttl;

    private Boolean setIfAbsent;

    private LocalTime expireTime;

    private boolean ttlKeyNotAbsent;

    public RedisCommandDescription(
            RedisCommand redisCommand,
            Integer ttl,
            LocalTime expireTime,
            Boolean setIfAbsent,
            Boolean ttlKeyNotAbsent) {
        super(redisCommand);
        this.expireTime = expireTime;
        this.ttl = ttl;
        this.setIfAbsent = setIfAbsent;
        this.ttlKeyNotAbsent = ttlKeyNotAbsent;
    }

    public Integer getTTL() {
        return ttl;
    }

    public LocalTime getExpireTime() {
        return expireTime;
    }

    public Boolean getSetIfAbsent() {
        return setIfAbsent;
    }

    public boolean getTtlKeyNotAbsent() {
        return ttlKeyNotAbsent;
    }
}
