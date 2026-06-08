package com.sziov.gacnev.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RowFactory;import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.*;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link EtlUtils} 测试用例。
 */
@Slf4j
@DisplayName("EtlUtils ETL工具测试")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class EtlUtilsTest {

    private static SparkSession spark;
    private static Dataset<Row> testDf;

    @BeforeAll
    static void setup() {
        spark = SparkSession.builder()
                .appName("EtlUtilsTest")
                .master("local[1]")
                .config("spark.ui.enabled", "false")
                .getOrCreate();

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

    @AfterAll
    static void teardown() {
        if (spark != null) {
            spark.stop();
        }
    }

    @Test
    @Order(1)
    @DisplayName("数据清洗应去除空格并转换空字符串为 null")
    void cleanData() {
        Dataset<Row> cleaned = EtlUtils.cleanData(testDf);

        // 验证首行 user_name 去除了首尾空格
        String userName = cleaned.select("user_name").collectAsList().get(0).getString(0);
        assertThat(userName).isEqualTo("张三");

        // 验证空字符串转为 null
        String email = cleaned.select("email").collectAsList().get(2).getString(0);
        assertThat(email).isNull();
    }

    @Test
    @Order(2)
    @DisplayName("邮箱脱敏")
    void maskEmail() {
        Dataset<Row> masked = EtlUtils.maskEmail(testDf, "email", "email_masked");
        String maskedEmail = masked.select("email_masked").collectAsList().get(0).getString(0);
        assertThat(maskedEmail).isEqualTo("tes****@example.com");
    }

    @Test
    @Order(3)
    @DisplayName("手机号脱敏")
    void maskPhone() {
        Dataset<Row> masked = EtlUtils.maskPhone(testDf, "phone", "phone_masked");
        String maskedPhone = masked.select("phone_masked").collectAsList().get(0).getString(0);
        assertThat(maskedPhone).isEqualTo("138****8000");
    }

    @Test
    @Order(4)
    @DisplayName("年龄分组")
    void groupByAge() {
        Dataset<Row> grouped = EtlUtils.groupByAge(testDf, "age", "age_group");
        String group = grouped.select("age_group").collectAsList().get(0).getString(0);
        assertThat(group).isEqualTo("青年");
    }

    @Test
    @Order(5)
    @DisplayName("过滤非空字段")
    void filterNotNull() {
        // 过滤 user_id 不为空
        Dataset<Row> filtered = EtlUtils.filterNotNull(testDf, "user_id");
        long count = filtered.count();
        assertThat(count).isEqualTo(4); // 第4行 user_id 为 null 被过滤

        // 同时过滤 user_id 和 user_name
        Dataset<Row> filteredBoth = EtlUtils.filterNotNull(testDf, new String[]{"user_id", "user_name"});
        assertThat(filteredBoth.count()).isEqualTo(4);
    }

    @Test
    @Order(6)
    @DisplayName("空值填充")
    void fillNull() {
        Dataset<Row> filled = EtlUtils.fillNull(testDf, "city", "未知");
        String city = filled.select("city").collectAsList().get(4).getString(0);
        assertThat(city).isEqualTo("未知");
    }

    @Test
    @Order(7)
    @DisplayName("添加处理时间戳")
    void addProcessTimestamp() {
        Dataset<Row> withTs = EtlUtils.addProcessTimestamp(testDf, "process_time");
        assertThat(withTs.schema().fieldNames()).contains("process_time");
    }
}
