package com.sziov.gacnev.datasource;

import com.sziov.gacnev.AbstractSparkTest;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("文件及 API 集成测试")
class DataSourceFileTest extends AbstractSparkTest {

    private static Dataset<Row> sampleDf() {
        return spark.createDataFrame(Arrays.asList(
                RowFactory.create(1, "Alice"),
                RowFactory.create(2, "Bob")
        ), new StructType(new org.apache.spark.sql.types.StructField[]{
                DataTypes.createStructField("id", DataTypes.IntegerType, false),
                DataTypes.createStructField("name", DataTypes.StringType, true)
        }));
    }

    private static Path newTempDir() throws IOException {
        return Files.createTempDirectory("spark_test_");
    }

    @Test
    @DisplayName("CSV_read_write_往返正确")
    void csv_readWrite() throws IOException {
        Path dir = newTempDir();
        DataSources.csv().option(o -> o.setWriteMode(SaveMode.Overwrite)).write(sampleDf(), dir.toString());
        Dataset<Row> result = DataSources.csv().read(spark, dir.toString());
        result.show();
        assertThat(result.count()).isEqualTo(2);
    }

    @Test
    @DisplayName("JSON_read_write_往返正确")
    void json_readWrite() throws IOException {
        Path dir = newTempDir();
        DataSources.json().option(o -> o.setWriteMode(SaveMode.Overwrite)).write(sampleDf(), dir.toString());
        Dataset<Row> result = DataSources.json().read(spark, dir.toString());
        result.show();
        assertThat(result.count()).isEqualTo(2);
    }

    @Test
    @DisplayName("Parquet_read_write_往返正确")
    void parquet_readWrite() throws IOException {
        Path dir = newTempDir();
        DataSources.parquet().option(o -> o.setWriteMode(SaveMode.Overwrite)).write(sampleDf(), dir.toString());
        Dataset<Row> result = DataSources.parquet().read(spark, dir.toString());
        result.show();
        assertThat(result.count()).isEqualTo(2);
    }

    @Test
    @DisplayName("ORC_read_write_往返正确")
    void orc_readWrite() throws IOException {
        Path dir = newTempDir();
        DataSources.orc().option(o -> o.setWriteMode(SaveMode.Overwrite)).write(sampleDf(), dir.toString());
        Dataset<Row> result = DataSources.orc().read(spark, dir.toString());
        result.show();
        assertThat(result.count()).isEqualTo(2);
    }

    @Test
    @DisplayName("Text_read_write_往返正确")
    void text_readWrite() throws IOException {
        Path dir = newTempDir();
        DataSources.text().option(o -> o.setWriteMode(SaveMode.Overwrite))
                .write(sampleDf().select("name"), dir.toString());
        Dataset<Row> result = DataSources.text().read(spark, dir.toString());
        result.show();
        assertThat(result.count()).isEqualTo(2);
    }

    @Test
    @DisplayName("option链式调用_分隔符设置_生效")
    void optionChain_delimiter() throws IOException {
        Path dir = newTempDir();
        DataSources.csv()
                .option(o -> o.setDelimiter("|").setWriteMode(SaveMode.Overwrite))
                .write(sampleDf(), dir.toString());
        assertThat(dir.toFile().list()).isNotEmpty();
    }

    @Test
    @DisplayName("write_带Consumer_覆盖之前的option")
    void write_withConsumer() throws IOException {
        Path dir = newTempDir();
        DataSources.csv()
                .option(o -> o.setWriteMode(SaveMode.Append))
                .write(sampleDf(), o -> o.setResource(dir.toString()).setWriteMode(SaveMode.Overwrite));
        assertThat(dir.toFile().list()).isNotEmpty();
    }
}
