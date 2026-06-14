package com.sziov.gacnev.utils.etl;

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
 * {@link EtlUtils} 测试用例。
 *
 * @author maikou
 * @since 2026-06-09
 */
@DisplayName("EtlUtils ETL工具测试")
class EtlUtilsTest extends AbstractSparkTest {

    private static Dataset<Row> testDf;

    @BeforeAll
    static void createTestData() {
        StructType schema = new StructType(new StructField[]{
                DataTypes.createStructField("user_id", DataTypes.StringType, true),
                DataTypes.createStructField("user_name", DataTypes.StringType, true),
                DataTypes.createStructField("email", DataTypes.StringType, true),
                DataTypes.createStructField("phone", DataTypes.StringType, true),
                DataTypes.createStructField("age", DataTypes.IntegerType, true),
                DataTypes.createStructField("city", DataTypes.StringType, true)
        });
        testDf = spark.createDataFrame(java.util.Arrays.asList(
                RowFactory.create("001", " 张三 ", "test@example.com", "13800138000", 25, "北京"),
                RowFactory.create("002", "李四 ", "user@test.com", "13900139000", 30, "上海"),
                RowFactory.create("003", "  王五  ", "", "", 17, ""),
                RowFactory.create(null, null, "noemail", "invalid", null, null),
                RowFactory.create("004", "赵六", "abc@test.com", "13700137000", 45, null)
        ), schema);
    }

    @Test
    @DisplayName("cleanData_含空格空字符串_去空格并转null")
    void cleanData_withWhitespaceAndEmpty_trimsAndNullifies() {
        Dataset<Row> cleaned = EtlUtils.cleanData(testDf);
        String userName = cleaned.select("user_name").collectAsList().get(0).getString(0);
        assertThat(userName).isEqualTo("张三");
        String email = cleaned.select("email").collectAsList().get(2).getString(0);
        assertThat(email).isNull();
    }

    @Test
    @DisplayName("maskEmail_有效邮箱_脱敏返回")
    void maskEmail_validEmail_masksCorrectly() {
        Dataset<Row> masked = EtlUtils.maskEmail(testDf, "email", "email_masked");
        String maskedEmail = masked.select("email_masked").collectAsList().get(0).getString(0);
        assertThat(maskedEmail).isEqualTo("tes****@example.com");
    }

    @Test
    @DisplayName("maskPhone_有效手机号_脱敏返回")
    void maskPhone_validPhone_masksCorrectly() {
        Dataset<Row> masked = EtlUtils.maskPhone(testDf, "phone", "phone_masked");
        String maskedPhone = masked.select("phone_masked").collectAsList().get(0).getString(0);
        assertThat(maskedPhone).isEqualTo("138****8000");
    }

    @Test
    @DisplayName("groupByAge_25岁_返回青年")
    void groupByAge_25_returnsYouth() {
        Dataset<Row> grouped = EtlUtils.groupByAge(testDf, "age", "age_group");
        String group = grouped.select("age_group").collectAsList().get(0).getString(0);
        assertThat(group).isEqualTo("青年");
    }

    @Test
    @DisplayName("groupByAge_45岁_返回中年")
    void groupByAge_45_returnsMiddleAge() {
        Dataset<Row> grouped = EtlUtils.groupByAge(testDf, "age", "age_group");
        String group = grouped.select("age_group").collectAsList().get(4).getString(0);
        assertThat(group).isEqualTo("中年");
    }

    @Test
    @DisplayName("filterNotNull_userId列_过滤null行")
    void filterNotNull_userIdColumn_filtersNullRows() {
        Dataset<Row> filtered = EtlUtils.filterNotNull(testDf, "user_id");
        assertThat(filtered.count()).isEqualTo(4);
    }

    @Test
    @DisplayName("fillNull_city列为空_填充默认值")
    void fillNull_cityColumnNull_fillsDefault() {
        Dataset<Row> filled = EtlUtils.fillNull(testDf, "city", "未知");
        String city = filled.select("city").collectAsList().get(4).getString(0);
        assertThat(city).isEqualTo("未知");
    }

    @Test
    @DisplayName("addProcessTimestamp_DataFrame_增加处理时间列")
    void addProcessTimestamp_dataFrame_addsTimestampColumn() {
        Dataset<Row> withTs = EtlUtils.addProcessTimestamp(testDf, "process_time");
        assertThat(withTs.schema().fieldNames()).contains("process_time");
    }
}
