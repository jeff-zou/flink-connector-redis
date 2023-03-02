package org.apache.flink.streaming.connectors.redis.table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.connectors.redis.common.config.RedisOptions;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.streaming.connectors.redis.descriptor.RedisValidator.REDIS_COMMAND;

/** Created by jeff.zou on 2020/9/10. */
public class RedisDynamicTableFactory
        implements DynamicTableSinkFactory, DynamicTableSourceFactory {

    public static final String IDENTIFIER = "redis";

    public static final String CACHE_SEPERATOR = "\01";

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        if (context.getCatalogTable().getOptions().containsKey(REDIS_COMMAND)) {
            context.getCatalogTable()
                    .getOptions()
                    .put(
                            REDIS_COMMAND,
                            context.getCatalogTable()
                                    .getOptions()
                                    .get(REDIS_COMMAND)
                                    .toUpperCase());
        }
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        ReadableConfig config = helper.getOptions();
        helper.validate();
        validateConfigOptions(config);
        return new RedisDynamicTableSource(
                context.getCatalogTable().getOptions(),
                context.getCatalogTable().getResolvedSchema(),
                config);
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        if (context.getCatalogTable().getOptions().containsKey(REDIS_COMMAND)) {
            context.getCatalogTable()
                    .getOptions()
                    .put(
                            REDIS_COMMAND,
                            context.getCatalogTable()
                                    .getOptions()
                                    .get(REDIS_COMMAND)
                                    .toUpperCase());
        }
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        ReadableConfig config = helper.getOptions();
        helper.validate();
        validateConfigOptions(config);
        return new RedisDynamicTableSink(
                context.getCatalogTable().getOptions(),
                context.getCatalogTable().getResolvedSchema(),
                config);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(RedisOptions.DATABASE);
        options.add(RedisOptions.HOST);
        options.add(RedisOptions.PORT);
        options.add(RedisOptions.MAXIDLE);
        options.add(RedisOptions.MAXTOTAL);
        options.add(RedisOptions.CLUSTERNODES);
        options.add(RedisOptions.PASSWORD);
        options.add(RedisOptions.TIMEOUT);
        options.add(RedisOptions.MINIDLE);
        options.add(RedisOptions.COMMAND);
        options.add(RedisOptions.REDISMODE);
        options.add(RedisOptions.TTL);
        options.add(RedisOptions.LOOKUP_CACHE_MAX_ROWS);
        options.add(RedisOptions.LOOKUP_CHCHE_TTL);
        options.add(RedisOptions.LOOKUP_MAX_RETRIES);
        options.add(RedisOptions.SINK_MAX_RETRIES);
        options.add(RedisOptions.SINK_PARALLELISM);
        options.add(RedisOptions.LOOKUP_CACHE_LOAD_ALL);
        options.add(RedisOptions.SINK_LIMIT);
        options.add(RedisOptions.SINK_LIMIT_MAX_NUM);
        options.add(RedisOptions.SINK_LIMIT_MAX_ONLINE);
        options.add(RedisOptions.SINK_LIMIT_INTERVAL);
        options.add(RedisOptions.VALUE_DATA_STRUCTURE);
        options.add(RedisOptions.REDIS_MASTER_NAME);
        options.add(RedisOptions.SENTINELS_INFO);
        options.add(RedisOptions.EXPIRE_ON_TIME);
        return options;
    }

    private void validateConfigOptions(ReadableConfig config) {}
}
