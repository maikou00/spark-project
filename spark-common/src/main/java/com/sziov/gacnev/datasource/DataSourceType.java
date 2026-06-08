package com.sziov.gacnev.datasource;

/**
 * 数据源类型枚举。
 *
 * @author maikou
 * @since 2026-06-09
 */
public enum DataSourceType {
    HIVE,
    CSV, JSON, PARQUET, ORC, TEXT,
    CLICKHOUSE,
    ELASTICSEARCH, MONGODB, REDIS,
    KAFKA
}
