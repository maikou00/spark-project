package com.sziov.gacnev.datasource.core;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

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
}
