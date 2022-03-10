package org.apache.flink.streaming.connectors.redis.common.util;

public class CheckUtil {
    public static void checkArgument(boolean condition, String message) {
        if(!condition) {
            throw new IllegalArgumentException(message);
        }
    }
}
