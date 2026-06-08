package com.sziov.gacnev.example;

import com.sziov.gacnev.utils.EtlUtils;
import com.sziov.gacnev.utils.SparkEnvUtils;
import com.sziov.gacnev.utils.SparkParameterTool;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Properties;

/**
 * JSON转CSV案例
 * <p>演示如何读取本地JSON数据、处理数据、保存为CSV文件</p>
 *
 * @author maikou
 * @since 2026-05-18
 */
@Slf4j
public class JsonToCsvExample {

    private static final String INPUT_JSON_PATH = "data/input/users.json";
    private static final String OUTPUT_CSV_PATH = "data/output/users.csv";
    public static void main(String[] args) {
        Properties parameters = SparkParameterTool.fromArgs(args);
        SparkSession spark = SparkEnvUtils.prepare(args, "JsonToCsvExample");

        Dataset<Row> jsonData = readJsonData(spark, parameters);
        Dataset<Row> etlData = etlProcessData(jsonData);
        writeCsvData(etlData, parameters);

        spark.stop();
    }

    /**
     * 读取JSON数据
     */
    private static Dataset<Row> readJsonData(SparkSession spark, Properties parameters) {
        // 定义Schema（生产环境建议显式定义Schema，避免自动推断带来的问题）
        StructType schema = new StructType(new StructField[]{
                DataTypes.createStructField("user_id", DataTypes.StringType, true),
                DataTypes.createStructField("user_name", DataTypes.StringType, true),
                DataTypes.createStructField("age", DataTypes.IntegerType, true),
                DataTypes.createStructField("gender", DataTypes.StringType, true),
                DataTypes.createStructField("email", DataTypes.StringType, true),
                DataTypes.createStructField("phone", DataTypes.StringType, true),
                DataTypes.createStructField("address", DataTypes.StringType, true),
                DataTypes.createStructField("create_time", DataTypes.StringType, true)
        });

        // 读取JSON文件
        String inputPath = SparkParameterTool.get(parameters, "input.path", INPUT_JSON_PATH);
        Dataset<Row> df = spark.read()
                .schema(schema)
                .option("multiline", "true")
                .option("mode", "FAILFAST")
                .json(inputPath);

        log.info("JSON数据读取完成，记录数: {}", df.count());
        return df;
    }

    /**
     * 数据预处理
     * <p>包括：数据清洗、数据转换、数据加密、数据过滤</p>
     */
    private static Dataset<Row> etlProcessData(Dataset<Row> df) {
        log.info("预处理前记录数: {}", df.count());
        // 数据清洗：去除字符串字段的首尾空格，空字符串转为null
        Dataset<Row> etlDF = EtlUtils.cleanData(df);
        // 数据转换：添加处理时间戳、数据脱敏
        etlDF = EtlUtils.addProcessTimestamp(etlDF, "process_time");
        etlDF = EtlUtils.maskEmail(etlDF, "email", "email_masked");
        etlDF = EtlUtils.maskPhone(etlDF, "phone", "phone_masked");
        etlDF = EtlUtils.groupByAge(etlDF, "age", "age_group");
        // 数据过滤：过滤无效数据
        etlDF = EtlUtils.filterNotNull(etlDF, new String[] {"user_id", "user_name"});

        log.info("预处理完成，剩余记录数: {}", etlDF.count());
        return etlDF;
    }

    // 业务逻辑指标计算等

    /**
     * 写入CSV文件
     */
    private static void writeCsvData(Dataset<Row> df, Properties parameters) {
        // 选择输出字段（排除敏感字段）
        String[] outputColumns = {
                "user_id",
                "user_name",
                "age",
                "age_group",
                "gender",
                "email_masked",
                "phone_masked",
                "address",
                "create_time",
                "process_time"
        };

        Dataset<Row> outputDf = df.selectExpr(outputColumns);

        // 写入CSV文件
        String outputPath = SparkParameterTool.get(parameters, "output.path", OUTPUT_CSV_PATH);
        outputDf.write()
                .mode("overwrite")
                .option("header", "true")
                .option("delimiter", ",")
                .option("encoding", "UTF-8")
                .option("quoteAll", "false")
                .option("escape", "\"")
                .option("nullValue", "")
                .csv(outputPath);

        log.info("CSV文件写入完成，输出路径: {}", outputPath);
    }
}
