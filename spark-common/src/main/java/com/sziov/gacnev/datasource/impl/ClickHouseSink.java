package com.sziov.gacnev.datasource.impl;

import com.sziov.gacnev.common.RetryUtils;
import com.sziov.gacnev.common.WarehouseException;
import com.sziov.gacnev.constant.ParamsKeyConstant;
import com.sziov.gacnev.datasource.DataSink;
import com.sziov.gacnev.datasource.DataSources;
import com.sziov.gacnev.datasource.option.ClickHouseOption;
import com.sziov.gacnev.spark.SparkParameterTool;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

import java.util.Properties;

/**
 * ClickHouse 数据写入。
 *
 * @author maikou
 * @since 2026-06-09
 */
@Slf4j
public class ClickHouseSink implements DataSink<ClickHouseOption> {

    private static final int DEFAULT_RETRIES = 3;

    @Override
    public void write(Dataset<Row> df, ClickHouseOption options) {
        Properties dsConfig = DataSources.getDsConfig();
        String jdbcUrl = SparkParameterTool.get(dsConfig, ParamsKeyConstant.DATASOURCE_CK_HOSTS, null);
        if (jdbcUrl == null || jdbcUrl.isEmpty()) {
            throw new WarehouseException("ClickHouse 连接地址未配置，请在 app.properties 中设置 datasource.ck.hosts");
        }
        String username = SparkParameterTool.get(dsConfig, ParamsKeyConstant.DATASOURCE_CK_USERNAME, "default");
        String password = SparkParameterTool.get(dsConfig, ParamsKeyConstant.DATASOURCE_CK_PASSWORD, "");

        Properties jdbcProps = new Properties();
        jdbcProps.setProperty("user", username);
        jdbcProps.setProperty("password", password);
        SaveMode mode = options.getWriteMode() != null ? options.getWriteMode() : SaveMode.Append;

        RetryUtils.retry(DEFAULT_RETRIES, 1000L, () -> {
            log.info("写入 ClickHouse，表: {}，模式: {}", options.getResource(), mode);
            if (SaveMode.Overwrite.equals(mode)) {
                java.sql.DriverManager.getConnection(jdbcUrl, jdbcProps)
                        .createStatement().execute("TRUNCATE TABLE " + options.getResource());
            }
            df.write().mode("append").jdbc(jdbcUrl, options.getResource(), jdbcProps);
            return null;
        });
    }
}
