package com.sziov.gacnev.datasource.option;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;
import org.apache.spark.sql.SaveMode;

/**
 * Hive 数据源 Option，包含读和写参数。
 *
 * @author maikou
 * @since 2026-06-11
 */
@Data
@Accessors(chain = true)
@NoArgsConstructor
@AllArgsConstructor
public class HiveOption implements DataSourceOption<HiveOption> {

    /** 资源标识：表名 */
    private String resource;

    /** 数据库名 */
    private String database;

    /** 分区过滤条件（读，如 dt='2026-06-10'） */
    private String partitionFilter;

    /** 写入模式 */
    private SaveMode writeMode;
}
