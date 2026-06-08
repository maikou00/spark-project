package com.sziov.gacnev.datasource.core;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.spark.sql.types.StructType;

/**
 * 统一读取选项。
 *
 * @author maikou
 * @since 2026-06-09
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ReadOptions {
    private String resource;
    private String query;
    private String database;
    private String partitionFilter;
    private String format;
    private StructType schema;
    private String delimiter;
    private String encoding;
    private String columnName;
    private int numPartitions;
}
