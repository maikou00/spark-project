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
    CLICKHOUSE, DORIS,
    /** TODO: 待实现 */
    ELASTICSEARCH, /** TODO: 待实现 */
    MONGODB, MYSQL, REDIS,
    /** TODO: 待实现 */
    KAFKA
}
