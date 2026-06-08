package com.sziov.gacnev.etl;

import com.sziov.gacnev.AbstractSparkTest;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link DataQEUtils} 测试用例。
 *
 * @author maikou
 * @since 2026-06-09
 */
@DisplayName("DataQEUtils 数据质量测试")
class DataQEUtilsTest extends AbstractSparkTest {

    private static Dataset<Row> testDf;

    @BeforeAll
    static void createTestData() {
        StructType schema = new StructType(new StructField[]{
                DataTypes.createStructField("id", DataTypes.IntegerType, false),
                DataTypes.createStructField("name", DataTypes.StringType, true),
                DataTypes.createStructField("score", DataTypes.IntegerType, true)
        });
        testDf = spark.createDataFrame(java.util.Arrays.asList(
                RowFactory.create(1, "Alice", 95),
                RowFactory.create(2, "Bob", 80),
                RowFactory.create(3, "Alice", 95),
                RowFactory.create(4, null, 60)
        ), schema);
    }

    @Test
    @DisplayName("getNullRatio_name列_返回null比例")
    void getNullRatio_nameColumn_returnsRatio() {
        double ratio = DataQEUtils.getNullRatio(testDf, "name");
        assertThat(ratio).isGreaterThan(0).isLessThanOrEqualTo(1.0);
    }

    @Test
    @DisplayName("columnExists_存在列_返回true")
    void columnExists_existingColumn_returnsTrue() {
        assertThat(DataQEUtils.columnExists(testDf, "id")).isTrue();
    }

    @Test
    @DisplayName("columnExists_不存在列_返回false")
    void columnExists_nonExistingColumn_returnsFalse() {
        assertThat(DataQEUtils.columnExists(testDf, "not_exist")).isFalse();
    }

    @Test
    @DisplayName("getDuplicateCount_含重复行_返回重复数")
    void getDuplicateCount_withDuplicates_returnsCount() {
        long dupCount = DataQEUtils.getDuplicateCount(testDf);
        assertThat(dupCount).isGreaterThanOrEqualTo(0);
    }

    @Test
    @DisplayName("getDistinctCount_id列_返回去重数")
    void getDistinctCount_idColumn_returnsDistinctCount() {
        long distinct = DataQEUtils.getDistinctCount(testDf, "id");
        assertThat(distinct).isEqualTo(4);
    }

    @Test
    @DisplayName("checkCompleteness_非空列_返回true")
    void checkCompleteness_nonNullColumn_returnsTrue() {
        assertThat(DataQEUtils.checkCompleteness(testDf, "id")).isTrue();
    }

    @Test
    @DisplayName("checkCompleteness_含空列_返回false")
    void checkCompleteness_withNullColumn_returnsFalse() {
        assertThat(DataQEUtils.checkCompleteness(testDf, "name")).isFalse();
    }
}
