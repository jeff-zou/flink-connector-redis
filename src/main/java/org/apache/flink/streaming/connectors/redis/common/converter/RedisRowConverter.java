package org.apache.flink.streaming.connectors.redis.common.converter;

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Base64;

import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.getPrecision;

/** redis serialize . @Author: jeff.zou @Date: 2022/3/10.13:17 */
public class RedisRowConverter {

    private static final int TIMESTAMP_PRECISION_MIN = 0;
    private static final int TIMESTAMP_PRECISION_MAX = 3;

    public static Object dataTypeFromString(LogicalType fieldType, String result) {
        if (result == null) {
            return null;
        }
        return createDeserializer(fieldType).deserialize(result);
    }

    public static String rowDataToString(LogicalType fieldType, RowData rowData, Integer index) {
        if (rowData.isNullAt(index)) {
            return null;
        }
        return createSerializer(fieldType).serialize(rowData, index);
    }

    public static RedisDeserializationConverter createDeserializer(LogicalType fieldType) {
        int precision;
        switch (fieldType.getTypeRoot()) {
            case BIGINT:
            case INTERVAL_DAY_TIME:
                return Long::valueOf;
            case FLOAT:
                return Float::valueOf;
            case DOUBLE:
                return Double::valueOf;
            case CHAR:
            case VARCHAR:
                return BinaryStringData::fromString;
            case BOOLEAN:
                return Boolean::valueOf;
            case BINARY:
            case VARBINARY:
                return result -> Base64.getDecoder().decode(result);
            case DECIMAL:
                DecimalType decimalType = (DecimalType) fieldType;
                precision = decimalType.getPrecision();
                final int scale = decimalType.getScale();
                return result -> {
                    BigDecimal decimal = new BigDecimal(result);
                    return DecimalData.fromBigDecimal(decimal, precision, scale);
                };
            case TINYINT:
                return Byte::valueOf;
            case SMALLINT:
                return Short::valueOf;
            case INTEGER:
            case DATE:
            case INTERVAL_YEAR_MONTH:
                return Integer::valueOf;
            case TIME_WITHOUT_TIME_ZONE:
                precision = getPrecision(fieldType);
                if (precision < TIMESTAMP_PRECISION_MIN || precision > TIMESTAMP_PRECISION_MAX) {
                    throw new UnsupportedOperationException(
                            String.format(
                                    "The precision %s of Time type is out of range [%s, %s]",
                                    precision, TIMESTAMP_PRECISION_MIN, TIMESTAMP_PRECISION_MAX));
                }
                return Integer::valueOf;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                precision = getPrecision(fieldType);
                if (precision < TIMESTAMP_PRECISION_MIN || precision > TIMESTAMP_PRECISION_MAX) {
                    throw new UnsupportedOperationException(
                            String.format(
                                    "The precision %s of Timestamp is out of " + "range [%s, %s]",
                                    precision, TIMESTAMP_PRECISION_MIN, TIMESTAMP_PRECISION_MAX));
                }
                return result -> {
                    long milliseconds = Long.valueOf(result);
                    return TimestampData.fromEpochMillis(milliseconds);
                };
            default:
                throw new UnsupportedOperationException("Unsupported field type: " + fieldType);
        }
    }

    private static RedisSerializationConverter createSerializer(LogicalType fieldType) {
        int precision;
        switch (fieldType.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                return (rowData, index) -> rowData.getString(index).toString();
            case BOOLEAN:
                return (rowData, index) -> String.valueOf(rowData.getBoolean(index));
            case BINARY:
            case VARBINARY:
                return (rowData, index) ->
                        Base64.getEncoder().encodeToString(rowData.getBinary(index));
            case DECIMAL:
                DecimalType decimalType = (DecimalType) fieldType;
                precision = decimalType.getPrecision();
                final int scale = decimalType.getScale();
                return (rowData, index) -> {
                    BigDecimal decimal = rowData.getDecimal(index, precision, scale).toBigDecimal();
                    return decimal.toString();
                };
            case TINYINT:
                return (rowData, index) -> String.valueOf(rowData.getByte(index));
            case SMALLINT:
                return (rowData, index) -> String.valueOf(rowData.getShort(index));
            case INTEGER:
            case DATE:
            case INTERVAL_YEAR_MONTH:
                return (rowData, index) -> String.valueOf(rowData.getInt(index));
            case TIME_WITHOUT_TIME_ZONE:
                precision = getPrecision(fieldType);
                if (precision < TIMESTAMP_PRECISION_MIN || precision > TIMESTAMP_PRECISION_MAX) {
                    throw new UnsupportedOperationException(
                            String.format(
                                    "The precision %s of Time type is out of range [%s, %s]",
                                    precision, TIMESTAMP_PRECISION_MIN, TIMESTAMP_PRECISION_MAX));
                }
                return (rowData, index) -> String.valueOf(rowData.getInt(index));
            case BIGINT:
            case INTERVAL_DAY_TIME:
                return (rowData, index) -> String.valueOf(rowData.getLong(index));
            case FLOAT:
                return (rowData, index) -> String.valueOf(rowData.getFloat(index));
            case DOUBLE:
                return (rowData, index) -> String.valueOf(rowData.getDouble(index));
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                precision = getPrecision(fieldType);
                if (precision < TIMESTAMP_PRECISION_MIN || precision > TIMESTAMP_PRECISION_MAX) {
                    throw new UnsupportedOperationException(
                            String.format(
                                    "The precision %s of Timestamp is out of range [%s, %s]",
                                    precision, TIMESTAMP_PRECISION_MIN, TIMESTAMP_PRECISION_MAX));
                }
                return (rowData, index) ->
                        String.valueOf(rowData.getTimestamp(index, precision).getMillisecond());
            default:
                throw new UnsupportedOperationException("Unsupported field type: " + fieldType);
        }
    }

    @FunctionalInterface
    interface RedisDeserializationConverter extends Serializable {
        Object deserialize(String field);
    }

    @FunctionalInterface
    interface RedisSerializationConverter extends Serializable {
        String serialize(RowData rowData, Integer index);
    }
}
