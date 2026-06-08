package com.sziov.gacnev.datasource.core;

/**
 * 数据源类型枚举。
 *
 * @author maikou
 * @since 2026-06-09
 */
public enum DataSourceType {
    HIVE,
    FILE_CSV, FILE_JSON, FILE_PARQUET, FILE_ORC, FILE_TEXT,
    CLICKHOUSE,
    ELASTICSEARCH, MONGODB, REDIS,
    KAFKA
}
