package com.sziov.gacnev.common;

import lombok.Getter;

/**
 * 数仓异常类
 * 统一的数仓异常处理
 *
 * @author maikou
 * @since 2026-05-17
 */
@Getter
public class WarehouseException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    /**
     * 异常错误码
     */
    private final String errorCode;

    /**
     * 构造函数
     *
     * @param message 异常消息
     */
    public WarehouseException(String message) {
        super(message);
        this.errorCode = "WAREHOUSE_ERROR";
    }

    /**
     * 构造函数
     *
     * @param message   异常消息
     * @param errorCode 错误码
     */
    public WarehouseException(String message, String errorCode) {
        super(message);
        this.errorCode = errorCode;
    }

    /**
     * 构造函数
     *
     * @param message 异常消息
     * @param cause   异常原因
     */
    public WarehouseException(String message, Throwable cause) {
        super(message, cause);
        this.errorCode = "WAREHOUSE_ERROR";
    }

    /**
     * 构造函数
     *
     * @param message   异常消息
     * @param errorCode 错误码
     * @param cause     异常原因
     */
    public WarehouseException(String message, String errorCode, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
    }

    /**
     * 创建数据源连接异常
     *
     * @param datasourceName 数据源名称
     * @return WarehouseException
     */
    public static WarehouseException connectionError(String datasourceName) {
        return new WarehouseException("Failed to connect to datasource: " + datasourceName, "CONNECTION_ERROR");
    }

    /**
     * 创建SQL执行异常
     *
     * @param sql SQL语句
     * @return WarehouseException
     */
    public static WarehouseException sqlExecutionError(String sql) {
        return new WarehouseException("Failed to execute SQL: " + sql, "SQL_EXECUTION_ERROR");
    }

    /**
     * 创建数据质量异常
     *
     * @param tableName 表名称
     * @param reason    原因
     * @return WarehouseException
     */
    public static WarehouseException dataQualityError(String tableName, String reason) {
        return new WarehouseException("Data quality error in table " + tableName + ": " + reason, "DATA_QUALITY_ERROR");
    }

    /**
     * 创建分区异常
     *
     * @param tableName 表名称
     * @param partition 分区规格
     * @return WarehouseException
     */
    public static WarehouseException partitionError(String tableName, String partition) {
        return new WarehouseException("Partition error in table " + tableName + ": " + partition, "PARTITION_ERROR");
    }

    /**
     * 创建文件操作异常
     *
     * @param filePath 文件路径
     * @param operation 操作类型
     * @return WarehouseException
     */
    public static WarehouseException fileOperationError(String filePath, String operation) {
        return new WarehouseException("Failed to " + operation + " file: " + filePath, "FILE_OPERATION_ERROR");
    }

    /**
     * 创建参数校验异常
     *
     * @param paramName 参数名称
     * @param reason    原因
     * @return WarehouseException
     */
    public static WarehouseException validationError(String paramName, String reason) {
        return new WarehouseException("Parameter validation error: " + paramName + " - " + reason, "VALIDATION_ERROR");
    }
}