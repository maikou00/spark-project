package com.sziov.gacnev.datasource.option;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;
import org.apache.spark.sql.SaveMode;

import java.util.List;

/**
 * MySQL 数据源 Option，包含读和写参数。
 *
 * @author maikou
 * @since 2026-06-11
 */
@Data
@Accessors(chain = true)
@NoArgsConstructor
@AllArgsConstructor
public class MySqlOption implements DataSourceOption<MySqlOption> {

    /** 资源标识：表名 */
    private String resource;

    /** SQL 查询/执行语句 */
    private String query;

    /** 写入模式，默认 Append */
    private SaveMode writeMode;

    /** 分区读取列名 */
    private String partitionColumn;

    /** 分区下界 */
    private Long lowerBound;

    /** 分区上界 */
    private Long upperBound;

    /** 分区数，默认 10 */
    private Integer numPartitions;

    /** 谓词分区条件列表（用于字符串/日期列分区并行读） */
    private List<String> predicates;

    /** UPSERT 唯一键列名列表 */
    private List<String> upsertKeys;
}
