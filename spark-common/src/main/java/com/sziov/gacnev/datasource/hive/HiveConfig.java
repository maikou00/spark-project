package com.sziov.gacnev.datasource.hive;

import lombok.Data;

import java.util.Collections;
import java.util.Map;

/**
 * Hive 配置。
 *
 * @author maikou
 * @since 2026-06-09
 */
@Data
public class HiveConfig {
    private Map<String, String> extraOptions = Collections.emptyMap();

    public Map<String, String> toSparkOptions() {
        return extraOptions;
    }
}
