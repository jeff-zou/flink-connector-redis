package org.apache.flink.streaming.connectors.redis.common.config;

import org.apache.flink.streaming.connectors.redis.common.util.CheckUtil;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import java.io.Serializable;

/** Base class for Flink Redis configuration. */
public abstract class FlinkJedisConfigBase implements Serializable {
    private static final long serialVersionUID = 1L;

    protected final int maxTotal;
    protected final int maxIdle;
    protected final int minIdle;
    protected final int connectionTimeout;
    protected final String password;

    protected FlinkJedisConfigBase(
            int connectionTimeout, int maxTotal, int maxIdle, int minIdle, String password) {
        CheckUtil.checkArgument(connectionTimeout >= 0, "connection timeout can not be negative");
        CheckUtil.checkArgument(maxTotal >= 0, "maxTotal value can not be negative");
        CheckUtil.checkArgument(maxIdle >= 0, "maxIdle value can not be negative");
        CheckUtil.checkArgument(minIdle >= 0, "minIdle value can not be negative");

        this.connectionTimeout = connectionTimeout;
        this.maxTotal = maxTotal;
        this.maxIdle = maxIdle;
        this.minIdle = minIdle;
        this.password = password;
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
     * Get the value for the {@code maxTotal} configuration attribute for pools to be created with
     * this configuration instance.
     *
     * @return The current setting of {@code maxTotal} for this configuration instance
     * @see GenericObjectPoolConfig#getMaxTotal()
     */
    public int getMaxTotal() {
        return maxTotal;
    }

    /**
     * Get the value for the {@code maxIdle} configuration attribute for pools to be created with
     * this configuration instance.
     *
     * @return The current setting of {@code maxIdle} for this configuration instance
     * @see GenericObjectPoolConfig#getMaxIdle()
     */
    public int getMaxIdle() {
        return maxIdle;
    }

    /**
     * Get the value for the {@code minIdle} configuration attribute for pools to be created with
     * this configuration instance.
     *
     * @return The current setting of {@code minIdle} for this configuration instance
     * @see GenericObjectPoolConfig#getMinIdle()
     */
    public int getMinIdle() {
        return minIdle;
    }

    /**
     * Returns password.
     *
     * @return password
     */
    public String getPassword() {
        return password;
    }

    @Override
    public String toString() {
        return "FlinkJedisConfigBase{"
                + "maxTotal="
                + maxTotal
                + ", maxIdle="
                + maxIdle
                + ", minIdle="
                + minIdle
                + ", connectionTimeout="
                + connectionTimeout
                + ", password='"
                + password
                + '\''
                + '}';
    }
}
