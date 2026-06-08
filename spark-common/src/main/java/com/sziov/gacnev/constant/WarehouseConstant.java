package com.sziov.gacnev.constant;

/**
 * 离线数仓全局常量
 * 分层、分区、存储格式、压缩、过期策略、时区
 *
 * @author maikou
 * @date 2026/05/17 17:51
 **/
public final class WarehouseConstant {

    // 数仓库名
    public static final String DB_ODS = "ods";
    public static final String DB_DWD = "dwd";
    public static final String DB_DWS = "dws";
    public static final String DB_ADS = "ads";
    public static final String DB_DIM = "dim";

    // 分区字段
    public static final String PART_DT = "dt";
    public static final String PART_HOUR = "hour";
    public static final String PART_MONTH = "month";
    public static final String PART_WEEK = "week";

    // 存储格式
    public static final String FORMAT_ORC = "orc";
    public static final String FORMAT_PARQUET = "parquet";
    // 压缩算法
    public static final String COMPRESS_SNAPPY = "snappy";
    public static final String COMPRESS_GZIP = "gzip";

    // 数据保留周期
    public static final int RETENTION_DAY_90 = 90;
    public static final int RETENTION_DAY_30 = 30;

    // 统一时区
    public static final String TIME_ZONE = "Asia/Shanghai";

    // 私有构造
    private WarehouseConstant() {
        throw new UnsupportedOperationException("常量类不允许实例化");
    }
}
