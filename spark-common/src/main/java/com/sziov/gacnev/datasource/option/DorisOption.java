package com.sziov.gacnev.datasource.option;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;
import org.apache.spark.sql.SaveMode;

import java.util.List;

/**
 * Doris 数据源 Option。
 *
 * @author maikou
 * @since 2026-06-12
 */
@Data
@Accessors(chain = true)
@NoArgsConstructor
@AllArgsConstructor
public class DorisOption implements DataSourceOption<DorisOption> {

    /** 资源标识：database.table */
    private String resource;

    /** SQL 查询语句 */
    private String query;

    /** 写入模式，默认 Append */
    private SaveMode writeMode;

    /** 分区过滤谓词列表（如 {@code ["dt >= '2026-06-01'", "dt < '2026-06-02'"]}） */
    private List<String> predicates;
}
