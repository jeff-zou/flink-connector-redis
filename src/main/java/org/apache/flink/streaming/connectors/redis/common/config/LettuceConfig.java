package org.apache.flink.streaming.connectors.redis.common.config;

import java.io.Serializable;

public class LettuceConfig implements Serializable {

    private static final long serialVersionUID = 1L;
    private final Integer nettyIoPoolSize;

    private final Integer nettyEventPoolSize;

    public LettuceConfig(Integer nettyIoPoolSize, Integer nettyEventPoolSize) {
        this.nettyIoPoolSize = nettyIoPoolSize;
        this.nettyEventPoolSize = nettyEventPoolSize;
    }

    public Integer getNettyIoPoolSize() {
        return nettyIoPoolSize;
    }

    public Integer getNettyEventPoolSize() {
        return nettyEventPoolSize;
    }

    @Override
    public String toString() {
        return "LettuceConfig{"
                + "nettyIoPoolSize="
                + nettyIoPoolSize
                + ", nettyEventPoolSize="
                + nettyEventPoolSize
                + '}';
    }
}
