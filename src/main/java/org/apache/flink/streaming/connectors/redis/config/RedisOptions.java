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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/** Created by jeff.zou on 2020/9/10. */
public class RedisOptions {

    private RedisOptions() {
    }

    public static final ConfigOption<Integer> TIMEOUT =
            ConfigOptions.key("timeout")
                    .intType()
                    .defaultValue(2000)
                    .withDescription("Optional timeout for connect to redis");

    public static final ConfigOption<Integer> MAXTOTAL =
            ConfigOptions.key("maxTotal")
                    .intType()
                    .defaultValue(2)
                    .withDescription("Optional maxTotal for connect to redis");

    public static final ConfigOption<Integer> MAXIDLE =
            ConfigOptions.key("maxIdle")
                    .intType()
                    .defaultValue(2)
                    .withDescription("Optional maxIdle for connect to redis");

    public static final ConfigOption<Integer> MINIDLE =
            ConfigOptions.key("minIdle")
                    .intType()
                    .defaultValue(1)
                    .withDescription("Optional minIdle for connect to redis");

    public static final ConfigOption<String> PASSWORD =
            ConfigOptions.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Optional password for connect to redis");

    public static final ConfigOption<Integer> PORT =
            ConfigOptions.key("port")
                    .intType()
                    .defaultValue(6379)
                    .withDescription("Optional port for connect to redis");

    public static final ConfigOption<String> HOST =
            ConfigOptions.key("host")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Optional host for connect to redis");

    public static final ConfigOption<Boolean> SSL =
            ConfigOptions.key("ssl")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Optional ssl for connect to redis");

    public static final ConfigOption<String> CLUSTERNODES =
            ConfigOptions.key("cluster-nodes")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Optional nodes for connect to redis cluster");

    public static final ConfigOption<Integer> DATABASE =
            ConfigOptions.key("database")
                    .intType()
                    .defaultValue(0)
                    .withDescription("Optional database for connect to redis");

    public static final ConfigOption<String> COMMAND =
            ConfigOptions.key("command")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Optional command for connect to redis");

    public static final ConfigOption<String> REDISMODE =
            ConfigOptions.key("redis-mode")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Optional redis-mode for connect to redis");

    public static final ConfigOption<String> REDIS_MASTER_NAME =
            ConfigOptions.key("master.name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Optional master.name for connect to redis sentinels");

    public static final ConfigOption<String> SENTINELS_INFO =
            ConfigOptions.key("sentinels.info")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Optional sentinels.info for connect to redis sentinels");

    public static final ConfigOption<String> SENTINELS_PASSWORD =
            ConfigOptions.key("sentinels.password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Optional sentinels.password for connect to redis sentinels");

    public static final ConfigOption<Integer> TTL =
            ConfigOptions.key("ttl")
                    .intType()
                    .noDefaultValue()
                    .withDescription("Optional ttl for insert to redis");

    public static final ConfigOption<Long> LOOKUP_CACHE_MAX_ROWS =
            ConfigOptions.key("lookup.cache.max-rows")
                    .longType()
                    .defaultValue(-1L)
                    .withDescription("Optional  max rows of cache for query redis");

    public static final ConfigOption<Long> LOOKUP_CHCHE_TTL =
            ConfigOptions.key("lookup.cache.ttl")
                    .longType()
                    .defaultValue(-1L)
                    .withDescription("Optional ttl of cache for query redis");

    public static final ConfigOption<Boolean> LOOKUP_CACHE_LOAD_ALL =
            ConfigOptions.key("lookup.cache.load-all")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Optional if load all elements into cache for query");

    public static final ConfigOption<Integer> MAX_RETRIES =
            ConfigOptions.key("max.retries")
                    .intType()
                    .defaultValue(1)
                    .withDescription("Optional max retries of cache sink");

    public static final ConfigOption<Integer> SINK_PARALLELISM =
            ConfigOptions.key("sink.parallelism")
                    .intType()
                    .defaultValue(1)
                    .withDescription("Optional parrallelism for sink");

    public static final ConfigOption<Boolean> SINK_LIMIT =
            ConfigOptions.key("sink.limit")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Optional if open the limit for sink ");

    public static final ConfigOption<Integer> SINK_LIMIT_MAX_NUM =
            ConfigOptions.key("sink.limit.max-num")
                    .intType()
                    .defaultValue(10000)
                    .withDescription("Optional the max num of writes for limited sink");

    public static final ConfigOption<Long> SINK_LIMIT_INTERVAL =
            ConfigOptions.key("sink.limit.interval")
                    .longType()
                    .defaultValue(100L)
                    .withDescription(
                            "Optional the millisecond interval between each write for limited sink");

    public static final ConfigOption<Long> SINK_LIMIT_MAX_ONLINE =
            ConfigOptions.key("sink.limit.max-online")
                    .longType()
                    .defaultValue(30 * 60 * 1000L)
                    .withDescription("Optional the max online milliseconds for limited sink");

    public static final ConfigOption<RedisValueDataStructure> VALUE_DATA_STRUCTURE =
            ConfigOptions.key("value.data.structure")
                    .enumType(RedisValueDataStructure.class)
                    .defaultValue(RedisValueDataStructure.column)
                    .withDescription("Optional redis value data structure.");

    public static final ConfigOption<String> EXPIRE_ON_TIME =
            ConfigOptions.key("ttl.on.time")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Optional redis key expire on time, eg: 10:00 12:12:01");

    public static final ConfigOption<Boolean> SET_IF_ABSENT =
            ConfigOptions.key("set.if.absent")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Optional setIfAbsent for insert(set/hset) to redis");

    public static final ConfigOption<Boolean> TTL_KEY_NOT_ABSENT =
            ConfigOptions.key("ttl.key.not.absent")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Optional set ttl when key not absent");

    public static final ConfigOption<Integer> NETTY_IO_POOL_SIZE =
            ConfigOptions.key("io.pool.size")
                    .intType()
                    .defaultValue(null)
                    .withDescription("Optional set io pool size for netty of lettuce");

    public static final ConfigOption<Integer> NETTY_EVENT_POOL_SIZE =
            ConfigOptions.key("event.pool.size")
                    .intType()
                    .defaultValue(null)
                    .withDescription("Optional set event pool size for netty of lettuce");

    public static final ConfigOption<String> SCAN_KEY =
            ConfigOptions.key("scan.key")
                    .stringType()
                    .defaultValue(null)
                    .withDescription("Optional set key for query");

    public static final ConfigOption<String> SCAN_ADDITION_KEY =
            ConfigOptions.key("scan.addition.key")
                    .stringType()
                    .defaultValue(null)
                    .withDescription("Optional set addition key for query");

    public static final ConfigOption<Integer> SCAN_RANGE_START =
            ConfigOptions.key("scan.range.start")
                    .intType()
                    .defaultValue(null)
                    .withDescription("Optional set range start for lrange query");
    public static final ConfigOption<Integer> SCAN_RANGE_STOP =
            ConfigOptions.key("scan.range.stop")
                    .intType()
                    .defaultValue(null)
                    .withDescription("Optional set range stop for lrange query");

    public static final ConfigOption<Integer> SCAN_COUNT =
            ConfigOptions.key("scan.count")
                    .intType()
                    .defaultValue(null)
                    .withDescription("Optional set count for srandmember query");

    public static final ConfigOption<String> ZREM_RANGEBY =
            ConfigOptions.key("zset.zremrangeby")
                    .stringType()
                    .defaultValue(null)
                    .withDescription("Remove related elements，Valid values: LEX,RANK,SCORE");
}
