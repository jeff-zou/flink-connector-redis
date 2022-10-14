package org.apache.flink.streaming.connectors.redis.common.config;

import org.apache.flink.streaming.connectors.redis.common.util.CheckUtil;

import java.io.Serializable;

/** Base class for Flink Redis configuration. */
public abstract class FlinkConfigBase implements Serializable {
    private static final long serialVersionUID = 1L;

    protected final int connectionTimeout;

    protected final String password;

    protected FlinkConfigBase(int connectionTimeout, String password) {
        CheckUtil.checkArgument(connectionTimeout >= 0, "connection timeout can not be negative");
        this.password = password;
        this.connectionTimeout = connectionTimeout;
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

    @Override
    public String toString() {
        return "FlinkConfigBase{"
                + "connectionTimeout="
                + connectionTimeout
                + ", password='"
                + password
                + '\''
                + '}';
    }
}
