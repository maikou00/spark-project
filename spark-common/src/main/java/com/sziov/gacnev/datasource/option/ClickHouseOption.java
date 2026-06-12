package com.sziov.gacnev.datasource.option;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;
import org.apache.spark.sql.SaveMode;

import java.util.List;

/**
 * ClickHouse 数据源 Option，包含读和写参数。
 *
 * @author maikou
 * @since 2026-06-11
 */
@Data
@Accessors(chain = true)
@NoArgsConstructor
@AllArgsConstructor
public class ClickHouseOption implements DataSourceOption<ClickHouseOption> {

    /** 资源标识：表名 */
    private String resource;

    /** SQL 查询/执行语句 */
    private String query;

    /** 写入模式 */
    private SaveMode writeMode;

    /** UPSERT 唯一键列名列表 */
    private List<String> upsertKeys;
}
