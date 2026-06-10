package com.sziov.gacnev.datasource.core;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.HashMap;
import java.util.Map;

/**
 * 统一写入选项。
 *
 * @author maikou
 * @since 2026-06-09
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class WriteOptions {
    private String resource;
    private String writeMode;
    private int repartitionNum;
    private int batchSize;
    /**
     * 分区值，覆盖默认的当前日期。
     * 用于历史数据回刷场景。
     */
    private String partitionValue;

    /**
     * 将 WriteOptions 转换为 Spark option 键值对。
     *
     * @return Spark 写入选项 Map
     */
    public Map<String, String> toSparkOptions() {
        Map<String, String> opts = new HashMap<>();
        if (batchSize > 0) {
            opts.put("batchsize", String.valueOf(batchSize));
        }
        if (repartitionNum > 0) {
            opts.put("repartitionNum", String.valueOf(repartitionNum));
        }
        return opts;
    }
}
