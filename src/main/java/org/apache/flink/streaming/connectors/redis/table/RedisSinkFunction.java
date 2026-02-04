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

package org.apache.flink.streaming.connectors.redis.table;

import io.lettuce.core.Range;
import io.lettuce.core.RedisFuture;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.redis.command.RedisCommand;
import org.apache.flink.streaming.connectors.redis.command.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.command.RedisInsertCommand;
import org.apache.flink.streaming.connectors.redis.config.FlinkConfigBase;
import org.apache.flink.streaming.connectors.redis.config.RedisOptions;
import org.apache.flink.streaming.connectors.redis.config.RedisValueDataStructure;
import org.apache.flink.streaming.connectors.redis.config.ZremType;
import org.apache.flink.streaming.connectors.redis.container.RedisCommandsContainer;
import org.apache.flink.streaming.connectors.redis.container.RedisCommandsContainerBuilder;
import org.apache.flink.streaming.connectors.redis.converter.RedisRowConverter;
import org.apache.flink.streaming.connectors.redis.mapper.RedisSinkMapper;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * @param <IN>
 */
public class RedisSinkFunction<IN> extends RichSinkFunction<IN> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(RedisSinkFunction.class);
    private final int maxRetryTimes;
    private final boolean setIfAbsent;
    private final boolean ttlKeyNotAbsent;
    private final RedisSinkMapper<IN> redisSinkMapper;
    private final RedisCommand redisCommand;
    private final FlinkConfigBase flinkConfigBase;
    private final List<DataType> columnDataTypes;
    private final RedisValueDataStructure redisValueDataStructure;
    private final String zremrangeby;
    private final boolean auditLog;
    protected Integer defaultTtl;
    protected int expireTimeSeconds = -1;
    private transient RedisCommandsContainer redisCommandsContainer;
    private final int ttlMetadataPosition;

    /**
     * Creates a new {@link RedisSinkFunction} that connects to the Redis server.
     *
     * @param flinkConfigBase The configuration of {@link FlinkConfigBase}
     * @param redisSinkMapper This is used to generate Redis command and key value from incoming
     *     elements.
     */
    public RedisSinkFunction(
            FlinkConfigBase flinkConfigBase,
            RedisSinkMapper<IN> redisSinkMapper,
            ResolvedSchema resolvedSchema,
            ReadableConfig readableConfig) {
        this(flinkConfigBase, redisSinkMapper, resolvedSchema, readableConfig, -1);
    }

    public RedisSinkFunction(
            FlinkConfigBase flinkConfigBase,
            RedisSinkMapper<IN> redisSinkMapper,
            ResolvedSchema resolvedSchema,
            ReadableConfig readableConfig,
            int ttlMetadataPosition) {
        Objects.requireNonNull(flinkConfigBase, "Redis connection pool config should not be null");
        Objects.requireNonNull(redisSinkMapper, "Redis Mapper can not be null");

        this.flinkConfigBase = flinkConfigBase;
        this.maxRetryTimes = readableConfig.get(RedisOptions.MAX_RETRIES);
        this.redisSinkMapper = redisSinkMapper;
        RedisCommandDescription redisCommandDescription =
                (RedisCommandDescription) redisSinkMapper.getCommandDescription();
        Preconditions.checkNotNull(
                redisCommandDescription, "Redis Mapper data type description can not be null");

        this.redisCommand = redisCommandDescription.getRedisCommand();
        this.defaultTtl = redisCommandDescription.getTTL();
        this.ttlKeyNotAbsent = redisCommandDescription.getTtlKeyNotAbsent();
        this.setIfAbsent = redisCommandDescription.getSetIfAbsent();
        this.auditLog = redisCommandDescription.isAuditLog();
        if (redisCommandDescription.getExpireTime() != null) {
            this.expireTimeSeconds = redisCommandDescription.getExpireTime().toSecondOfDay();
        }

        this.columnDataTypes = resolvedSchema.getColumnDataTypes();
        this.redisValueDataStructure = readableConfig.get(RedisOptions.VALUE_DATA_STRUCTURE);
        this.zremrangeby = readableConfig.get(RedisOptions.ZREM_RANGEBY);
        this.ttlMetadataPosition = ttlMetadataPosition;
    }

    /**
     * Called when new data arrives to the sink, and forwards it to Redis channel. Depending on the
     * specified Redis data type, a different Redis command will be applied. Available commands are
     * RPUSH, LPUSH, SADD, PUBLISH, SET, SETEX, PFADD, HSET, ZADD.
     *
     * @param input The incoming data
     */
    @Override
    public void invoke(IN input, Context context) throws Exception {
        RowData rowData = (RowData) input;
        RowKind kind = rowData.getRowKind();
        if (kind == RowKind.UPDATE_BEFORE) {
            return;
        }

        // Handle TTL from metadata if available
        Integer effectiveTtl = this.defaultTtl;
        if (ttlMetadataPosition >= 0 && ttlMetadataPosition < rowData.getArity()
                && !rowData.isNullAt(ttlMetadataPosition)) {
            int ttlFromMetadata = rowData.getInt(ttlMetadataPosition);
            if (ttlFromMetadata > 0) {
                effectiveTtl = ttlFromMetadata;
            }
        }

        String[] params = new String[calcParamNumByCommand(rowData.getArity())];
        for (int i = 0; i < params.length; i++) {
            params[i] =
                    redisSinkMapper.getKeyFromData(
                            rowData, columnDataTypes.get(i).getLogicalType(), i);
        }

        // the value is taken from the entire row when redisValueFromType is row, and columns
        // separated by '\01'
        if (redisValueDataStructure == RedisValueDataStructure.row) {
            params[params.length - 1] = serializeWholeRow(rowData);
        }
        startSink(params, kind, effectiveTtl);
        if (auditLog) {
            LOG.info("{}", rowData);
        }
    }

    /**
     * It will try many times which less than {@code maxRetryTimes} until execute success.
     *
     * @param params
     * @throws Exception
     */
    private void startSink(String[] params, RowKind kind, Integer ttl) throws Exception {
        for (int i = 0; i <= maxRetryTimes; i++) {
            try {
                RedisFuture redisFuture = null;
                if (kind == RowKind.DELETE) {
                    redisFuture = rowKindDelete(params);
                } else {
                    redisFuture = sink(params);
                }

                if (redisFuture != null) {
                    redisFuture.whenComplete((r, t) -> setTtl(params[0], ttl));
                }

                break;
            } catch (UnsupportedOperationException e) {
                throw e;
            } catch (Exception e1) {
                LOG.error("sink redis error, retry times:{}", i, e1);
                if (i >= this.maxRetryTimes) {
                    throw new RuntimeException("sink redis error ", e1);
                }
                Thread.sleep(500L * i);
            }
        }
    }

    /**
     * process redis command.
     *
     * @param params
     */
    private RedisFuture sink(String[] params) {
        RedisFuture redisFuture = null;
        switch (redisCommand.getInsertCommand()) {
            case RPUSH:
                redisFuture = this.redisCommandsContainer.rpush(params[0], params[1]);
                break;
            case LPUSH:
                redisFuture = this.redisCommandsContainer.lpush(params[0], params[1]);
                break;
            case SADD:
                redisFuture = this.redisCommandsContainer.sadd(params[0], params[1]);
                break;
            case SET:
                {
                    if (!this.setIfAbsent) {
                        redisFuture = this.redisCommandsContainer.set(params[0], params[1]);
                    } else {
                        redisFuture = this.redisCommandsContainer.exists(params[0]);
                        redisFuture.whenComplete(
                                (existsVal, throwable) -> {
                                    if ((int) existsVal == 0) {
                                        this.redisCommandsContainer.set(params[0], params[1]);
                                    }
                                });
                    }
                }
                break;
            case PFADD:
                redisFuture = this.redisCommandsContainer.pfadd(params[0], params[1]);
                break;
            case PUBLISH:
                redisFuture = this.redisCommandsContainer.publish(params[0], params[1]);
                break;
            case ZADD:
                redisFuture =
                        this.redisCommandsContainer.zadd(
                                params[0], Double.parseDouble(params[1]), params[2]);
                if (zremrangeby != null) {
                    redisFuture.whenComplete(
                            (ignore, throwable) -> {
                                try {
                                    if (zremrangeby.equalsIgnoreCase(ZremType.SCORE.name())) {
                                        Range<Double> range =
                                                Range.create(
                                                        Double.parseDouble(params[3]),
                                                        Double.parseDouble(params[4]));
                                        this.redisCommandsContainer.zremRangeByScore(
                                                params[0], range);
                                    } else if (zremrangeby.equalsIgnoreCase(ZremType.LEX.name())) {
                                        Range<String> range = Range.create(params[3], params[4]);
                                        this.redisCommandsContainer.zremRangeByLex(
                                                params[0], range);
                                    } else if (zremrangeby.equalsIgnoreCase(ZremType.RANK.name())) {
                                        this.redisCommandsContainer.zremRangeByRank(
                                                params[0],
                                                Long.parseLong(params[3]),
                                                Long.parseLong(params[4]));
                                    } else {
                                        LOG.warn("Unrecognized zrem type:{}", zremrangeby);
                                    }
                                } catch (Exception e) {
                                    LOG.error("{} zremRangeBy failed.", params[0], e);
                                }
                            });
                }
                break;
            case ZINCRBY:
                redisFuture =
                        this.redisCommandsContainer.zincrBy(
                                params[0], Double.valueOf(params[1]), params[2]);
                break;
            case ZREM:
                redisFuture = this.redisCommandsContainer.zrem(params[0], params[1]);
                break;
            case SREM:
                redisFuture = this.redisCommandsContainer.srem(params[0], params[1]);
                break;
            case HSET:
                {
                    if (!this.setIfAbsent) {
                        redisFuture =
                                this.redisCommandsContainer.hset(params[0], params[1], params[2]);
                    } else {
                        redisFuture = this.redisCommandsContainer.hexists(params[0], params[1]);
                        redisFuture.whenComplete(
                                (exist, throwable) -> {
                                    if (!(Boolean) exist) {
                                        this.redisCommandsContainer.hset(
                                                params[0], params[1], params[2]);
                                    }
                                });
                    }
                }
                break;
            case HMSET:
                {
                    if (params.length < 2) {
                        throw new RuntimeException("params length must be greater than 2");
                    }
                    if (params.length % 2 != 1) {
                        throw new RuntimeException("params length must be odd");
                    }
                    // 遍历把params第一个下标作为key，从第二个下标作为value，存进map中
                    Map<String, String> hashField = new HashMap<>();
                    for (int i = 1; i < params.length; i++) {
                        hashField.put(params[i], params[++i]);
                    }
                    if (!this.setIfAbsent) {
                        redisFuture = this.redisCommandsContainer.hmset(params[0], hashField);
                    } else {
                        redisFuture = this.redisCommandsContainer.exists(params[0]);
                        redisFuture.whenComplete(
                                (exist, throwable) -> {
                                    if (!(Boolean) exist) {
                                        this.redisCommandsContainer.hmset(params[0], hashField);
                                    }
                                });
                    }
                }
                break;
            case HINCRBY:
                redisFuture =
                        this.redisCommandsContainer.hincrBy(
                                params[0], params[1], Long.valueOf(params[2]));
                break;
            case HINCRBYFLOAT:
                redisFuture =
                        this.redisCommandsContainer.hincrByFloat(
                                params[0], params[1], Double.valueOf(params[2]));
                break;
            case INCRBY:
                redisFuture =
                        this.redisCommandsContainer.incrBy(params[0], Long.valueOf(params[1]));
                break;
            case INCRBYFLOAT:
                redisFuture =
                        this.redisCommandsContainer.incrByFloat(
                                params[0], Double.valueOf(params[1]));
                break;
            case DECRBY:
                redisFuture =
                        this.redisCommandsContainer.decrBy(params[0], Long.valueOf(params[1]));
                break;
            case DEL:
                redisFuture = this.redisCommandsContainer.del(params[0]);
                break;
            case HDEL:
                redisFuture = this.redisCommandsContainer.hdel(params[0], params[1]);
                break;
            default:
                throw new UnsupportedOperationException(
                        "Cannot process such data type: " + redisCommand);
        }
        return redisFuture;
    }

    /**
     * process redis command when RowKind == DELETE.
     *
     * @param params
     */
    private RedisFuture rowKindDelete(String[] params) {
        RedisFuture redisFuture = null;
        switch (redisCommand.getDeleteCommand()) {
            case SREM:
                redisFuture = this.redisCommandsContainer.srem(params[0], params[1]);
                break;
            case DEL:
                redisFuture = this.redisCommandsContainer.del(params[0]);
                break;
            case ZREM:
                redisFuture = this.redisCommandsContainer.zrem(params[0], params[2]);
                break;
            case ZINCRBY:
                Double d = -Double.valueOf(params[1]);
                redisFuture = this.redisCommandsContainer.zincrBy(params[0], d, params[2]);
                break;
            case HDEL:
                {
                    redisFuture = this.redisCommandsContainer.hdel(params[0], params[1]);
                }
                break;
            case HINCRBY:
                redisFuture =
                        this.redisCommandsContainer.hincrBy(
                                params[0], params[1], -Long.valueOf(params[2]));
                break;
            case HINCRBYFLOAT:
                redisFuture =
                        this.redisCommandsContainer.hincrByFloat(
                                params[0], params[1], -Double.valueOf(params[2]));
                break;
            case INCRBY:
                redisFuture =
                        this.redisCommandsContainer.incrBy(params[0], -Long.valueOf(params[1]));
                break;
            case INCRBYFLOAT:
                redisFuture =
                        this.redisCommandsContainer.incrByFloat(
                                params[0], -Double.valueOf(params[1]));
                break;
        }
        return redisFuture;
    }

    /**
     * set ttl for key.
     *
     * @param key
     */
    private void setTtl(String key, Integer ttl) {
        if (redisCommand == RedisCommand.DEL) {
            return;
        }

        if (ttl != null) {
            if (ttlKeyNotAbsent) {
                // set ttl when key not absent
                this.redisCommandsContainer
                        .getTTL(key)
                        .whenComplete(
                                (t, h) -> {
                                    if (t < 0) {
                                        this.redisCommandsContainer.expire(key, ttl);
                                    }
                                });
            } else {
                // set ttl every sink
                this.redisCommandsContainer.expire(key, ttl);
            }
        } else if (expireTimeSeconds != -1) {
            this.redisCommandsContainer
                    .getTTL(key)
                    .whenComplete(
                            (t, h) -> {
                                if (t < 0) {
                                    int now = LocalTime.now().toSecondOfDay();
                                    this.redisCommandsContainer.expire(
                                            key,
                                            expireTimeSeconds > now
                                                    ? expireTimeSeconds - now
                                                    : 86400 + expireTimeSeconds - now);
                                }
                            });
        }
    }

    /**
     * serialize whole row.
     *
     * @param rowData
     * @return
     */
    private String serializeWholeRow(RowData rowData) {
        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < columnDataTypes.size(); i++) {
            stringBuilder.append(
                    RedisRowConverter.rowDataToString(
                            columnDataTypes.get(i).getLogicalType(), rowData, i));
            if (i != columnDataTypes.size() - 1) {
                stringBuilder.append(RedisDynamicTableFactory.CACHE_SEPERATOR);
            }
        }
        return stringBuilder.toString();
    }

    /**
     * calculate the number of redis command's param
     *
     * @return
     */
    private int calcParamNumByCommand(int rowDataNum) {
        if (redisCommand == RedisCommand.DEL) {
            return 1;
        }

        if (redisCommand.getInsertCommand() == RedisInsertCommand.ZADD && zremrangeby != null) {
            return 5;
        } else if (redisCommand.getInsertCommand() == RedisInsertCommand.HSET
                || redisCommand.getInsertCommand() == RedisInsertCommand.ZADD
                || redisCommand.getInsertCommand() == RedisInsertCommand.HINCRBY
                || redisCommand.getInsertCommand() == RedisInsertCommand.HINCRBYFLOAT
                || redisCommand.getInsertCommand() == RedisInsertCommand.ZINCRBY) {
            return 3;
        } else if (redisCommand.getInsertCommand() == RedisInsertCommand.HMSET) {
            return rowDataNum;
        }

        return 2;
    }

    /**
     * Initializes the connection to Redis by either cluster or sentinels or single server.
     *
     * @throws IllegalArgumentException if PoolConfig, ClusterConfig and SentinelConfig are all null
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        Preconditions.checkArgument(
                redisCommand.getInsertCommand() != RedisInsertCommand.NONE,
                "the command %s do not support insert.",
                redisCommand.name());

        try {
            this.redisCommandsContainer = RedisCommandsContainerBuilder.build(this.flinkConfigBase);
            this.redisCommandsContainer.open();
            LOG.info("success to create redis container for sink");
        } catch (Exception e) {
            LOG.error("Redis has not been properly initialized: ", e);
            throw e;
        }
    }

    /**
     * Closes commands container.
     *
     * @throws IOException if command container is unable to close.
     */
    @Override
    public void close() throws IOException {
        if (redisCommandsContainer != null) {
            redisCommandsContainer.close();
        }
    }

    public int getTtlMetadataPosition() {
        return ttlMetadataPosition;
    }
}
