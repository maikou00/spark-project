package com.sziov.gacnev.datasource.file;

import lombok.Data;

import java.util.Collections;
import java.util.Map;

/**
 * 文件类型数据源配置。
 *
 * @author maikou
 * @since 2026-06-09
 */
@Data
public class FileConfig {
    private Map<String, String> extraOptions = Collections.emptyMap();

    public Map<String, String> toSparkOptions() {
        return extraOptions;
    }
}
