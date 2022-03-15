package org.apache.flink.streaming.connectors.redis.common.util;

/** check argument. */
public class CheckUtil {
    public static void checkArgument(boolean condition, String message) {
        if (!condition) {
            throw new IllegalArgumentException(message);
        }
    }
}
