package com.sziov.gacnev.datasource.file;

import com.sziov.gacnev.AbstractSparkTest;
import com.sziov.gacnev.datasource.core.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("FileSourceSink 文件读写测试")
class FileSourceSinkTest extends AbstractSparkTest {

    private static final StructType SCHEMA = new StructType(new StructField[]{
            DataTypes.createStructField("id", DataTypes.IntegerType, false),
            DataTypes.createStructField("name", DataTypes.StringType, true)
    });

    private static final FileConfig FILE_CONFIG = new FileConfig();

    @BeforeAll
    static void initRegistry() {
        DataSources.hive();
    }

    private static Dataset<Row> sampleDf() {
        return spark.createDataFrame(Arrays.asList(
                RowFactory.create(1, "Alice"),
                RowFactory.create(2, "Bob")
        ), SCHEMA);
    }

    private static Path newTempDir() throws IOException {
        return Files.createTempDirectory("spark_test_");
    }

    @Test
    @DisplayName("GenericSource_read_CSV_读取成功")
    void genericSource_read_csv_readsCorrectly() throws IOException {
        Path baseDir = newTempDir();
        java.io.File outDir = new java.io.File(baseDir.toFile(), "data");
        sampleDf().write().option("header", "true").csv(outDir.toString());
        ReadOptions opts = ReadOptions.builder().resource(outDir.toString()).build();
        Dataset<Row> df = new GenericSource<>("csv", FILE_CONFIG, FILE_CONFIG::toSparkOptions, null, 3)
                .read(spark, opts);
        assertThat(df.count()).isGreaterThanOrEqualTo(2);
    }

    @Test
    @DisplayName("GenericSink_write_CSV_文件生成")
    void genericSink_write_csv_writesFile() throws IOException {
        Path dir = newTempDir();
        WriteOptions opts = WriteOptions.builder().resource(dir.toString()).writeMode("overwrite").build();
        new GenericSink<>("csv", FILE_CONFIG, FILE_CONFIG::toSparkOptions, null, 3)
                .write(sampleDf(), opts);
        assertThat(dir.toFile().list()).isNotEmpty();
    }

    @Test
    @DisplayName("GenericSource_read_JSON_读取成功")
    void genericSource_read_json_readsCorrectly() throws IOException {
        Path baseDir = newTempDir();
        java.io.File outDir = new java.io.File(newTempDir().toFile(), "data");
        sampleDf().write().json(outDir.toString());
        ReadOptions opts = ReadOptions.builder().resource(outDir.toString()).build();
        Dataset<Row> df = new GenericSource<>("json", FILE_CONFIG, FILE_CONFIG::toSparkOptions, null, 3)
                .read(spark, opts);
        assertThat(df.count()).isEqualTo(2);
    }

    @Test
    @DisplayName("GenericSink_write_JSON_文件生成")
    void genericSink_write_json_writesFile() throws IOException {
        Path dir = newTempDir();
        WriteOptions opts = WriteOptions.builder().resource(dir.toString()).writeMode("overwrite").build();
        new GenericSink<>("json", FILE_CONFIG, FILE_CONFIG::toSparkOptions, null, 3)
                .write(sampleDf(), opts);
        assertThat(dir.toFile().list()).isNotEmpty();
    }

    @Test
    @DisplayName("GenericSource_read_Parquet_读取成功")
    void genericSource_read_parquet_readsCorrectly() throws IOException {
        Path dir = newTempDir();
        java.io.File outDir = new java.io.File(newTempDir().toFile(), "data");
        sampleDf().write().parquet(outDir.toString());
        ReadOptions opts = ReadOptions.builder().resource(outDir.toString()).build();
        Dataset<Row> df = new GenericSource<>("parquet", FILE_CONFIG, FILE_CONFIG::toSparkOptions, null, 3)
                .read(spark, opts);
        assertThat(df.count()).isEqualTo(2);
    }

    @Test
    @DisplayName("GenericSink_write_Parquet_文件生成")
    void genericSink_write_parquet_writesFile() throws IOException {
        Path dir = newTempDir();
        WriteOptions opts = WriteOptions.builder().resource(dir.toString()).writeMode("overwrite").build();
        new GenericSink<>("parquet", FILE_CONFIG, FILE_CONFIG::toSparkOptions, null, 3)
                .write(sampleDf(), opts);
        assertThat(dir.toFile().list()).isNotEmpty();
    }

    @Test
    @DisplayName("GenericSource_read_ORC_读取成功")
    void genericSource_read_orc_readsCorrectly() throws IOException {
        Path dir = newTempDir();
        java.io.File outDir = new java.io.File(newTempDir().toFile(), "data");
        sampleDf().write().orc(outDir.toString());
        ReadOptions opts = ReadOptions.builder().resource(outDir.toString()).build();
        Dataset<Row> df = new GenericSource<>("orc", FILE_CONFIG, FILE_CONFIG::toSparkOptions, null, 3)
                .read(spark, opts);
        assertThat(df.count()).isEqualTo(2);
    }

    @Test
    @DisplayName("GenericSink_write_ORC_文件生成")
    void genericSink_write_orc_writesFile() throws IOException {
        Path dir = newTempDir();
        WriteOptions opts = WriteOptions.builder().resource(dir.toString()).writeMode("overwrite").build();
        new GenericSink<>("orc", FILE_CONFIG, FILE_CONFIG::toSparkOptions, null, 3)
                .write(sampleDf(), opts);
        assertThat(dir.toFile().list()).isNotEmpty();
    }

    @Test
    @DisplayName("GenericSource_read_Text_读取成功")
    void genericSource_read_text_readsCorrectly() throws IOException {
        Path dir = newTempDir();
        java.io.File outDir = new java.io.File(newTempDir().toFile(), "data");
        sampleDf().select("name").write().text(outDir.toString());
        ReadOptions opts = ReadOptions.builder().resource(outDir.toString()).columnName("raw_text").build();
        Dataset<Row> df = new GenericSource<>("text", FILE_CONFIG, FILE_CONFIG::toSparkOptions, null, 3)
                .read(spark, opts);
        assertThat(df.count()).isEqualTo(2);
    }

    @Test
    @DisplayName("GenericSink_write_Text_文件生成")
    void genericSink_write_text_writesFile() throws IOException {
        Path dir = newTempDir();
        WriteOptions opts = WriteOptions.builder().resource(dir.toString()).writeMode("overwrite").build();
        new GenericSink<>("text", FILE_CONFIG, FILE_CONFIG::toSparkOptions, null, 3)
                .write(sampleDf().select("name"), opts);
        assertThat(dir.toFile().list()).isNotEmpty();
    }
}
