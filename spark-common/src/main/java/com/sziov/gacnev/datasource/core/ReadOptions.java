package com.sziov.gacnev.datasource.core;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.spark.sql.types.StructType;

import java.util.HashMap;
import java.util.Map;

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

    /**
     * 将 ReadOptions 转换为 Spark option 键值对。
     *
     * @return Spark 读取选项 Map
     */
    public Map<String, String> toSparkOptions() {
        Map<String, String> opts = new HashMap<>();
        if (delimiter != null) {
            opts.put("delimiter", delimiter);
        }
        if (encoding != null) {
            opts.put("encoding", encoding);
        }
        if (columnName != null) {
            opts.put("columnName", columnName);
        }
        if (numPartitions > 0) {
            opts.put("numPartitions", String.valueOf(numPartitions));
        }
        return opts;
    }
}
