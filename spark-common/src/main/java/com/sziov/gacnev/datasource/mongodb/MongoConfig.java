package com.sziov.gacnev.datasource.mongodb;

import com.sziov.gacnev.datasource.core.DataSourceConfig;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * MongoDB 配置。
 *
 * @author maikou
 * @since 2026-06-09
 */
@Data
@EqualsAndHashCode(callSuper = true)
public class MongoConfig extends DataSourceConfig {
    private String uri;
    private String database;

    public static MongoConfig fromProps(java.util.Properties props) {
        MongoConfig cfg = new MongoConfig();
        cfg.setUri(props.getProperty("datasource.mongo.uri", "mongodb://localhost:27017"));
        cfg.setDatabase(props.getProperty("datasource.mongo.database", ""));
        return cfg;
    }
}
