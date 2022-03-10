package org.apache.flink.streaming.connectors.redis.common.util;

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;

import java.math.BigDecimal;
import java.util.Base64;

/**
 * @author jeff.zou
 * @date 2022/3/10.13:17
 */
public class RedisSerializeUtil {

    public static Object dataTypeFromString(LogicalType fieldType, String result) {

        switch (fieldType.getTypeRoot()){
            case BIGINT:
            case INTERVAL_DAY_TIME:
                return Long.valueOf(result);
            case FLOAT:
                return Float.valueOf(result);
            case DOUBLE:
                return Double.valueOf(result);
            case CHAR:
            case VARCHAR:
                return BinaryStringData.fromString(result);
            case BOOLEAN:
                return Boolean.valueOf(result);
            case BINARY:
            case VARBINARY:
                return Base64.getDecoder().decode(result);
            case DECIMAL:
                DecimalType decimalType = (DecimalType) fieldType;
                final int precision = decimalType.getPrecision();
                final int scale = decimalType.getScale();
                BigDecimal decimal = new BigDecimal(result);
                return DecimalData.fromBigDecimal(decimal, precision, scale);
            case TINYINT:
                return Byte.valueOf(result);
            case SMALLINT:
                return Short.valueOf(result);
            case INTEGER:
            case DATE:
            case INTERVAL_YEAR_MONTH:
                return Integer.valueOf(result);
            default:
                throw new RuntimeException("unsupport file type: " + fieldType);
        }
    }
}