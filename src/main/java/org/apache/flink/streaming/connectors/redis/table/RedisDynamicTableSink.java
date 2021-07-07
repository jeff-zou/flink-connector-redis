package org.apache.flink.streaming.connectors.redis.table;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisConfigBase;
import org.apache.flink.streaming.connectors.redis.common.hanlder.FlinkJedisConfigHandler;
import org.apache.flink.streaming.connectors.redis.common.hanlder.RedisHandlerServices;
import org.apache.flink.streaming.connectors.redis.common.hanlder.RedisMapperHandler;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;

import java.util.Map;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * Created by jeff.zou on 2020/9/10.
 */
public class RedisDynamicTableSink implements DynamicTableSink {

    private FlinkJedisConfigBase flinkJedisConfigBase;
    private RedisMapper redisMapper;
    private Map<String, String> properties = null;
    private TableSchema tableSchema;
    private ReadableConfig config;

    public RedisDynamicTableSink(Map<String, String> properties,  TableSchema tableSchema, ReadableConfig config) {
        this.properties = properties;
        Preconditions.checkNotNull(properties, "properties should not be null");
        this.tableSchema = tableSchema;
        Preconditions.checkNotNull(tableSchema, "tableSchema should not be null");
        this.config = config;

        redisMapper = RedisHandlerServices
                .findRedisHandler(RedisMapperHandler.class, properties)
                .createRedisMapper(config);
        flinkJedisConfigBase = RedisHandlerServices
                .findRedisHandler(FlinkJedisConfigHandler.class, properties).createFlinkJedisConfig(config);
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        validatePrimaryKey(requestedMode);
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.DELETE)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .build();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        return SinkFunctionProvider.of(new RedisSink(flinkJedisConfigBase, redisMapper, tableSchema));
    }

    @Override
    public DynamicTableSink copy() {
        return new RedisDynamicTableSink(properties, tableSchema, config);
    }

    @Override
    public String asSummaryString() {
        return "REDIS";
    }

    private void validatePrimaryKey(ChangelogMode requestedMode) {
        checkState(ChangelogMode.insertOnly().equals(requestedMode),
                "please declare primary key for sink table when query contains update/delete record.");
    }
}