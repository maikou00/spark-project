package com.sziov.gacnev.datasource.option;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.StructType;

/**
 * 文件数据源 Option，包含读和写参数。
 *
 * @author maikou
 * @since 2026-06-11
 */
@Data
@Accessors(chain = true)
@NoArgsConstructor
@AllArgsConstructor
public class FileOption implements DataSourceOption<FileOption> {

    /** 资源标识：文件路径 */
    private String resource;

    /** 数据格式（csv/json/parquet/orc/text） */
    private String format;

    /** 分隔符（CSV 读） */
    private String delimiter;

    /** 字符编码（读） */
    private String encoding;

    /** 列名别名（Text 格式读） */
    private String columnName;

    /** 显式指定 Schema（读） */
    private StructType schema;

    /** 写入模式 */
    private SaveMode writeMode;

    /** 写入前重分区数 */
    private int repartitionNum;

    /** 批量写入大小 */
    private int batchSize;
}
