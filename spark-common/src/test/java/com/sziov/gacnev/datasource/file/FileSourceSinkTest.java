package com.sziov.gacnev.datasource.file;

import com.sziov.gacnev.AbstractSparkTest;
import com.sziov.gacnev.datasource.core.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
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
    @DisplayName("CsvSource_read_标准CSV_读取成功")
    void csvSource_read_standardCsv_readsCorrectly() throws IOException {
        Path baseDir = newTempDir();
        java.io.File outDir = new java.io.File(baseDir.toFile(), "data");
        sampleDf().write().option("header", "true").csv(outDir.toString());
        Path dir = outDir.toPath();
        ReadOptions opts = ReadOptions.builder().resource(dir.toString()).build();
        Dataset<Row> df = new CsvSource(new DataSourceConfig()).read(spark, opts);
        assertThat(df.count()).isEqualTo(2);
    }

    @Test
    @DisplayName("CsvSink_write_写入CSV_文件生成")
    void csvSink_write_standardDf_writesFile() throws IOException {
        Path dir = newTempDir();
        WriteOptions opts = WriteOptions.builder().resource(dir.toString()).writeMode("overwrite").build();
        new CsvSink(new DataSourceConfig()).write(sampleDf(), opts);
        assertThat(dir.toFile().list()).isNotEmpty();
    }

    @Test
    @DisplayName("JsonSource_read_标准JSON_读取成功")
    void jsonSource_read_standardJson_readsCorrectly() throws IOException {
        Path dir = newTempDir();
        java.io.File outDir = new java.io.File(newTempDir().toFile(), "data");
        sampleDf().write().json(outDir.toString());
        ReadOptions opts = ReadOptions.builder().resource(outDir.toString()).build();
        Dataset<Row> df = new JsonSource(new DataSourceConfig()).read(spark, opts);
        assertThat(df.count()).isEqualTo(2);
    }

    @Test
    @DisplayName("JsonSink_write_写入JSON_文件生成")
    void jsonSink_write_standardDf_writesFile() throws IOException {
        Path dir = newTempDir();
        WriteOptions opts = WriteOptions.builder().resource(dir.toString()).writeMode("overwrite").build();
        new JsonSink(new DataSourceConfig()).write(sampleDf(), opts);
        assertThat(dir.toFile().list()).isNotEmpty();
    }

    @Test
    @DisplayName("ParquetSource_read_标准Parquet_读取成功")
    void parquetSource_read_standardParquet_readsCorrectly() throws IOException {
        Path dir = newTempDir();
        java.io.File outDir = new java.io.File(newTempDir().toFile(), "data");
        sampleDf().write().parquet(outDir.toString());
        ReadOptions opts = ReadOptions.builder().resource(outDir.toString()).build();
        Dataset<Row> df = new ParquetSource(new DataSourceConfig()).read(spark, opts);
        assertThat(df.count()).isEqualTo(2);
    }

    @Test
    @DisplayName("ParquetSink_write_写入Parquet_文件生成")
    void parquetSink_write_standardDf_writesFile() throws IOException {
        Path dir = newTempDir();
        WriteOptions opts = WriteOptions.builder().resource(dir.toString()).writeMode("overwrite").repartitionNum(1).build();
        new ParquetSink(new DataSourceConfig()).write(sampleDf(), opts);
        assertThat(dir.toFile().list()).isNotEmpty();
    }

    @Test
    @DisplayName("OrcSource_read_标准ORC_读取成功")
    void orcSource_read_standardOrc_readsCorrectly() throws IOException {
        Path dir = newTempDir();
        java.io.File outDir = new java.io.File(newTempDir().toFile(), "data");
        sampleDf().write().orc(outDir.toString());
        ReadOptions opts = ReadOptions.builder().resource(outDir.toString()).build();
        Dataset<Row> df = new OrcSource(new DataSourceConfig()).read(spark, opts);
        assertThat(df.count()).isEqualTo(2);
    }

    @Test
    @DisplayName("OrcSink_write_写入ORC_文件生成")
    void orcSink_write_standardDf_writesFile() throws IOException {
        Path dir = newTempDir();
        WriteOptions opts = WriteOptions.builder().resource(dir.toString()).writeMode("overwrite").repartitionNum(1).build();
        new OrcSink(new DataSourceConfig()).write(sampleDf(), opts);
        assertThat(dir.toFile().list()).isNotEmpty();
    }

    @Test
    @DisplayName("TextSource_read_标准Text_读取成功")
    void textSource_read_standardText_readsCorrectly() throws IOException {
        Path dir = newTempDir();
        java.io.File outDir = new java.io.File(newTempDir().toFile(), "data");
        spark.createDataset(Arrays.asList("hello", "world"), org.apache.spark.sql.Encoders.STRING())
                .write().text(outDir.toString());
        ReadOptions opts = ReadOptions.builder().resource(outDir.toString()).columnName("line").build();
        Dataset<Row> df = new TextSource(new DataSourceConfig()).read(spark, opts);
        assertThat(df.count()).isEqualTo(2);
    }

    @Test
    @DisplayName("TextSink_write_写入Text_文件生成")
    void textSink_write_standardDf_writesFile() throws IOException {
        Path dir = newTempDir();
        WriteOptions opts = WriteOptions.builder().resource(dir.toString()).writeMode("overwrite").build();
        new TextSink(new DataSourceConfig()).write(
                spark.createDataset(Arrays.asList("a", "b"), org.apache.spark.sql.Encoders.STRING()).toDF(), opts);
        assertThat(dir.toFile().list()).isNotEmpty();
    }
}
