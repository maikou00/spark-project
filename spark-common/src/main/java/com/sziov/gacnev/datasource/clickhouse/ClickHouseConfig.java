package com.sziov.gacnev.datasource.clickhouse;

import com.sziov.gacnev.datasource.core.DataSourceConfig;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * ClickHouse 配置。
 *
 * @author maikou
 * @since 2026-06-09
 */
@Data
@EqualsAndHashCode(callSuper = true)
public class ClickHouseConfig extends DataSourceConfig {
    private String jdbcUrl;
    private String username = "default";
    private String password = "";
    private int batchSize = 10000;

    public static ClickHouseConfig fromProps(java.util.Properties props) {
        ClickHouseConfig cfg = new ClickHouseConfig();
        cfg.setHosts(props.getProperty("datasource.ck.hosts", "localhost:8123"));
        cfg.setJdbcUrl("jdbc:clickhouse://" + cfg.getHosts() + "?async_insert=1");
        cfg.setUsername(props.getProperty("datasource.ck.username", "default"));
        cfg.setPassword(props.getProperty("datasource.ck.password", ""));
        cfg.setBatchSize(Integer.parseInt(props.getProperty("datasource.ck.batch.size", "10000")));
        return cfg;
    }
}
