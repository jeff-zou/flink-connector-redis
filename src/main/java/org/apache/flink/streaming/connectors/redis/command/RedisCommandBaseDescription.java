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

import org.apache.flink.util.Preconditions;

import java.io.Serializable;

/** base description. @Author: Jeff.Zou @Date: 2022/3/9.14:55 */
public class RedisCommandBaseDescription implements Serializable {

    private static final long serialVersionUID = 1L;

    private final RedisCommand redisCommand;

    public RedisCommandBaseDescription(RedisCommand redisCommand) {
        Preconditions.checkNotNull(redisCommand, "redis command type cant be null!!!");
        this.redisCommand = redisCommand;
    }

    public RedisCommand getRedisCommand() {
        return redisCommand;
    }
}
