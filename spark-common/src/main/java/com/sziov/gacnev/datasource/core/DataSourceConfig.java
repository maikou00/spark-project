package com.sziov.gacnev.datasource.core;

import lombok.Data;

import java.util.HashMap;
import java.util.Map;

/**
 * 数据源连接配置基类。
 *
 * @author maikou
 * @since 2026-06-09
 */
@Data
public class DataSourceConfig {
    private String hosts;
    private int connectionTimeout = 30000;
    private int socketTimeout = 30000;
    private int maxRetries = 3;
    private Map<String, String> extraOptions = new HashMap<>();
}
