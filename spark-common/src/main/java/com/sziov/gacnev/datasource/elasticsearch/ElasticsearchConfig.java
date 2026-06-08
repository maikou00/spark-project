package com.sziov.gacnev.datasource.elasticsearch;

import com.sziov.gacnev.datasource.core.DataSourceConfig;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * Elasticsearch 配置。
 *
 * @author maikou
 * @since 2026-06-09
 */
@Data
@EqualsAndHashCode(callSuper = true)
public class ElasticsearchConfig extends DataSourceConfig {
    /** ES 端口 */
    private int port = 9200;
    /** 是否自动创建索引 */
    private boolean indexAutoCreate = true;
    /** 写入批量大小 */
    private int batchSize = 1000;

    public static ElasticsearchConfig fromProps(java.util.Properties props) {
        ElasticsearchConfig cfg = new ElasticsearchConfig();
        cfg.setHosts(props.getProperty("datasource.es.hosts", "localhost"));
        cfg.setPort(Integer.parseInt(props.getProperty("datasource.es.port", "9200")));
        cfg.setIndexAutoCreate(Boolean.parseBoolean(props.getProperty("datasource.es.index.auto.create", "true")));
        return cfg;
    }
}
