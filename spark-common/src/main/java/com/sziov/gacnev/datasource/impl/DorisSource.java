package com.sziov.gacnev.datasource.impl;

import com.sziov.gacnev.common.RetryUtils;
import com.sziov.gacnev.common.WarehouseException;
import com.sziov.gacnev.constant.ParamsDefaultValue;
import com.sziov.gacnev.constant.ParamsKeyConstant;
import com.sziov.gacnev.datasource.DataSource;
import com.sziov.gacnev.datasource.DataSources;
import com.sziov.gacnev.datasource.option.DorisOption;
import com.sziov.gacnev.spark.SparkParameterTool;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;
import java.util.Properties;

/**
 * Doris 数据读取，通过 Spark-Doris-Connector 直连 BE 并行扫描。
 *
 * @author maikou
 * @since 2026-06-12
 */
@Slf4j
public class DorisSource implements DataSource<DorisOption> {

    private static final int DEFAULT_RETRIES = 3;

    @Override
    public Dataset<Row> read(SparkSession spark, DorisOption options) {
        Properties dsConfig = DataSources.getDsConfig();
        String fenodes = SparkParameterTool.get(dsConfig, ParamsKeyConstant.DATASOURCE_DORIS_FENODES, null);
        if (fenodes == null || fenodes.isEmpty()) {
            throw new WarehouseException("Doris FE 地址未配置，请在 app.properties 中设置 datasource.doris.fenodes");
        }
        String resource = options.getResource();
        if (resource != null && !resource.contains(".")) {
            throw new WarehouseException("表名必须为 database.table 格式，当前: " + resource);
        }
        String username = SparkParameterTool.get(dsConfig,
                ParamsKeyConstant.DATASOURCE_DORIS_USERNAME, ParamsDefaultValue.DATASOURCE_DORIS_USERNAME);
        String password = SparkParameterTool.get(dsConfig,
                ParamsKeyConstant.DATASOURCE_DORIS_PASSWORD, ParamsDefaultValue.DATASOURCE_DORIS_PASSWORD);

        return RetryUtils.retry(DEFAULT_RETRIES, 1000L, () -> {
            log.info("从 Doris 读取数据（Connector），表: {}", resource);
            DataFrameReader reader = spark.read()
                    .format("doris")
                    .option("doris.fenodes", fenodes)
                    .option("doris.query.port", SparkParameterTool.get(dsConfig,
                    ParamsKeyConstant.DATASOURCE_DORIS_QUERY_PORT, ParamsDefaultValue.DATASOURCE_DORIS_QUERY_PORT))
                    .option("user", username)
                    .option("password", password == null ? "" : password);

            String tableIdentifier;
            if (options.getQuery() != null && !options.getQuery().isEmpty()) {
                tableIdentifier = options.getQuery();
            } else {
                tableIdentifier = resource;
            }
            reader = reader.option("doris.table.identifier", tableIdentifier);

            List<String> predicates = options.getPredicates();
            if (predicates != null && !predicates.isEmpty()) {
                reader = reader.option("doris.filter.query",
                        String.join(" and ", predicates));
            }

            return reader.load();
        });
    }
}
