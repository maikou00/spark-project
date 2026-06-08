package com.sziov.gacnev.utils;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.*;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * ReadDataUtils测试用例
 * 测试数据存放在data/input目录下
 *
 * @author maikou
 * @since 2026-05-19
 */
@Slf4j
public class ReadDataUtilsTest {

    private static final String DATA_INPUT_DIR = "data/input";
    private static final String JSON_FILE_NAME = "users.json";
    private static final String CSV_FILE_NAME = "users.csv";
    private static final String PARQUET_FILE_NAME = "users.parquet";
    private static final String ORC_FILE_NAME = "users.orc";
    private static final String TEXT_FILE_NAME = "users.txt";

    private static SparkSession spark;
    private static Path dataInputDir;
    private static Path jsonFile;
    private static Path csvFile;
    private static Path parquetFile;
    private static Path orcFile;
    private static Path textFile;

    /**
     * 测试前初始化SparkSession和测试文件
     */
    @BeforeAll
    static void setup() throws IOException {
        // 创建本地SparkSession
        spark = SparkSession.builder()
                .appName("ReadDataUtilsTest")
                .master("local[2]")
                .getOrCreate();
        
        // 创建data/input目录
        dataInputDir = Paths.get(DATA_INPUT_DIR);
        if (!Files.exists(dataInputDir)) {
            Files.createDirectories(dataInputDir);
        }

        // 创建测试JSON文件
        jsonFile = dataInputDir.resolve(JSON_FILE_NAME);
        createJsonFile(jsonFile);
        
        // 创建测试CSV文件
        csvFile = dataInputDir.resolve(CSV_FILE_NAME);
        createCsvFile(csvFile);
        
        // 创建测试文本文件
        textFile = dataInputDir.resolve(TEXT_FILE_NAME);
        createTextFile(textFile);
        
        // 创建测试Parquet文件
        parquetFile = dataInputDir.resolve(PARQUET_FILE_NAME);
        createParquetFile(parquetFile);
        
        // 创建测试ORC文件
        orcFile = dataInputDir.resolve(ORC_FILE_NAME);
        createOrcFile(orcFile);
    }

    /**
     * 测试后清理资源
     */
    @AfterAll
    static void teardown() {
        if (spark != null) {
            spark.stop();
        }
    }

    /**
     * 创建JSON测试文件
     */
    private static void createJsonFile(Path path) throws IOException {
        String content = "{\"user_id\":\"001\",\"user_name\":\"张三\",\"age\":25,\"gender\":\"男\"}\n" +
                        "{\"user_id\":\"002\",\"user_name\":\"李四\",\"age\":30,\"gender\":\"女\"}\n" +
                        "{\"user_id\":\"003\",\"user_name\":\"王五\",\"age\":28,\"gender\":\"男\"}";
        Files.write(path, content.getBytes());
        log.info("创建JSON测试文件: {}", path);
    }

    /**
     * 创建CSV测试文件
     */
    private static void createCsvFile(Path path) throws IOException {
        String content = "001,张三,25,男\n" +
                        "002,李四,30,女\n" +
                        "003,王五,28,男";
        Files.write(path, content.getBytes());
        log.info("创建CSV测试文件: {}", path);
    }

    /**
     * 创建文本测试文件
     */
    private static void createTextFile(Path path) throws IOException {
        String content = "Hello World\n" +
                        "Apache Spark\n" +
                        "Big Data Processing";
        Files.write(path, content.getBytes());
        log.info("创建文本测试文件: {}", path);
    }

    /**
     * 创建Parquet测试文件
     */
    private static void createParquetFile(Path path) {
        StructType schema = new StructType()
                .add("user_id", DataTypes.StringType)
                .add("user_name", DataTypes.StringType)
                .add("age", DataTypes.IntegerType);
        
        // 创建JavaBean列表
        java.util.List<UserData> data = java.util.Arrays.asList(
                new UserData("001", "张三", 25),
                new UserData("002", "李四", 30)
        );
        
        Dataset<Row> df = spark.createDataFrame(data, UserData.class);
        df = df.select("user_id", "user_name", "age");

        df.write().mode(SaveMode.Overwrite).parquet(path.toString());
        log.info("创建Parquet测试文件: {}", path);
    }

    /**
     * 创建ORC测试文件
     */
    private static void createOrcFile(Path path) {
        StructType schema = new StructType()
                .add("user_id", DataTypes.StringType)
                .add("user_name", DataTypes.StringType)
                .add("age", DataTypes.IntegerType);
        
        // 创建JavaBean列表
        java.util.List<UserData> data = java.util.Arrays.asList(
                new UserData("001", "张三", 25),
                new UserData("002", "李四", 30)
        );
        
        Dataset<Row> df = spark.createDataFrame(data, UserData.class);
        df = df.select("user_id", "user_name", "age");
        
        df.write().mode(SaveMode.Overwrite).orc(path.toString());
        log.info("创建ORC测试文件: {}", path);
    }

    /**
     * 测试读取JSON文件（自定义Schema）
     */
    @Test
    void testReadJsonWithSchema() {
        StructType schema = new StructType()
                .add("user_id", DataTypes.StringType)
                .add("user_name", DataTypes.StringType)
                .add("age", DataTypes.IntegerType)
                .add("gender", DataTypes.StringType);
        
        Dataset<Row> df = ReadDataUtils.readJsonWithSchema(spark, jsonFile.toString(), schema);
        
        log.info("Schema: {}", df.schema().treeString());
        log.info("数据行数: {}", df.count());
        log.info("数据内容:");
        df.show();
        
        assert df.count() == 3;
        assert df.schema().fields().length == 4;
        
        log.info("测试读取JSON文件完成");
    }

    /**
     * 测试读取CSV文件（自定义Schema）
     */
    @Test
    @DisplayName("测试读取CSV文件（自定义Schema）")
    void testReadCsvWithSchema() {
        log.info("开始测试读取CSV文件");
        
        StructType schema = new StructType()
                .add("user_id", DataTypes.StringType)
                .add("user_name", DataTypes.StringType)
                .add("age", DataTypes.IntegerType)
                .add("gender", DataTypes.StringType);
        
        Dataset<Row> df = ReadDataUtils.readCsvWithSchema(spark, csvFile.toString(), ",", schema);
        
        log.info("Schema: {}", df.schema().treeString());
        log.info("数据行数: {}", df.count());
        log.info("数据内容:");
        df.show();
        
        assert df.count() == 3;
        assert df.schema().fields().length == 4;
        
        log.info("测试读取CSV文件完成");
    }

    /**
     * 测试读取CSV文件（默认分隔符）
     */
    @Test
    @DisplayName("测试读取CSV文件（默认分隔符）")
    void testReadCsvWithDefaultDelimiter() {
        log.info("开始测试读取CSV文件（默认分隔符）");
        
        StructType schema = new StructType()
                .add("user_id", DataTypes.StringType)
                .add("user_name", DataTypes.StringType)
                .add("age", DataTypes.IntegerType)
                .add("gender", DataTypes.StringType);
        
        Dataset<Row> df = ReadDataUtils.readCsvWithSchema(spark, csvFile.toString(), null, schema);
        
        log.info("数据行数: {}", df.count());
        df.show();
        
        assert df.count() == 3;
        
        log.info("测试读取CSV文件（默认分隔符）完成");
    }

    /**
     * 测试读取Parquet文件
     */
    @Test
    @DisplayName("测试读取Parquet文件")
    void testReadParquet() {
        log.info("开始测试读取Parquet文件");
        
        Dataset<Row> df = ReadDataUtils.readParquet(spark, parquetFile.toString());
        
        log.info("Schema: {}", df.schema().treeString());
        log.info("数据行数: {}", df.count());
        log.info("数据内容:");
        df.show();
        
        assert df.count() == 2;
        assert df.schema().fields().length == 3;
        
        log.info("测试读取Parquet文件完成");
    }

    /**
     * 测试读取ORC文件
     */
    @Test
    @DisplayName("测试读取ORC文件")
    void testReadOrc() {
        log.info("开始测试读取ORC文件");
        
        Dataset<Row> df = ReadDataUtils.readOrc(spark, orcFile.toString());
        
        log.info("Schema: {}", df.schema().treeString());
        log.info("数据行数: {}", df.count());
        log.info("数据内容:");
        df.show();
        
        assert df.count() == 2;
        assert df.schema().fields().length == 3;
        
        log.info("测试读取ORC文件完成");
    }

    /**
     * 测试读取文本文件
     */
    @Test
    @DisplayName("测试读取文本文件")
    void testReadText() {
        log.info("开始测试读取文本文件");
        
        Dataset<Row> df = ReadDataUtils.readText(spark, textFile.toString());
        
        log.info("Schema: {}", df.schema().treeString());
        log.info("数据行数: {}", df.count());
        log.info("数据内容:");
        df.show();
        
        assert df.count() == 3;
        assert df.schema().fields().length == 1;
        assert df.schema().fields()[0].name().equals("raw_text");
        
        log.info("测试读取文本文件完成");
    }

    /**
     * 测试读取文本文件（自定义列名）
     */
    @Test
    @DisplayName("测试读取文本文件（自定义列名）")
    void testReadTextWithCustomColumnName() {
        log.info("开始测试读取文本文件（自定义列名）");
        
        Dataset<Row> df = ReadDataUtils.readText(spark, textFile.toString(), "content");
        
        log.info("Schema: {}", df.schema().treeString());
        log.info("数据行数: {}", df.count());
        log.info("数据内容:");
        df.show();
        
        assert df.count() == 3;
        assert df.schema().fields()[0].name().equals("content");
        
        log.info("测试读取文本文件（自定义列名）完成");
    }

    /**
     * 测试读取文本文件（默认列名）
     */
    @Test
    @DisplayName("测试读取文本文件（默认列名）")
    void testReadTextWithDefaultColumnName() {
        log.info("开始测试读取文本文件（默认列名）");
        
        Dataset<Row> df = ReadDataUtils.readText(spark, textFile.toString(), null);
        
        log.info("列名: {}", df.schema().fields()[0].name());
        
        assert df.schema().fields()[0].name().equals("raw_text");
        
        log.info("测试读取文本文件（默认列名）完成");
    }

    /**
     * 用户数据JavaBean
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class UserData implements java.io.Serializable {
        private String user_id;
        private String user_name;
        private Integer age;
    }
}