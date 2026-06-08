package com.sziov.gacnev;

import com.sziov.gacnev.utils.SparkEnvUtils;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;

/**
 * Spark 应用程序入口。
 *
 * <p>演示环境初始化、WordCount、DataFrame 操作等基础功能。
 * 生产作业应继承此模式：环境初始化 → 业务逻辑 → 资源清理。</p>
 *
 * @author maikou
 * @since 2026-05-16
 */
@Slf4j
public class App {

    public static void main(String[] args) {
        // 1. 初始化 Spark 环境
        SparkSession spark = SparkEnvUtils.prepare(args);

        // 2. 查看所有生效的配置
        SparkEnvUtils.getAllConfigMsg(spark);

        // 3. 执行示例
        try {
            wordCountExample(spark);
            dataFrameExample(spark);
        } catch (Exception e) {
            log.error("示例执行失败", e);
            System.exit(1);
        } finally {
            // 4. 清理
            spark.stop();
            log.info("Spark 应用已停止");
        }
    }

    /**
     * WordCount 示例。
     */
    private static void wordCountExample(SparkSession spark) {
        log.info("========== WordCount 示例开始 ==========");

        List<String> data = Arrays.asList(
                "Hello Spark",
                "Hello World",
                "Spark is awesome",
                "World is beautiful",
                "Hello Hello Hello"
        );

        Dataset<String> lines = spark.createDataset(data, Encoders.STRING());

        Dataset<Row> wordCounts = lines
                .flatMap((String line) -> Arrays.asList(line.split(" ")).iterator(), Encoders.STRING())
                .groupBy("value")
                .count()
                .orderBy(org.apache.spark.sql.functions.desc("count"));

        wordCounts.show();

        log.info("========== WordCount 示例结束 ==========");
    }

    /**
     * DataFrame 操作示例。
     */
    private static void dataFrameExample(SparkSession spark) {
        log.info("========== DataFrame 示例开始 ==========");

        List<Person> people = Arrays.asList(
                new Person("张三", 25, "北京"),
                new Person("李四", 30, "上海"),
                new Person("王五", 28, "广州"),
                new Person("赵六", 35, "深圳")
        );

        Dataset<Row> df = spark.createDataFrame(people, Person.class);
        df.show();

        log.info("年龄 > 28 的人员：");
        df.filter(df.col("age").gt(28)).show();

        df.describe("age").show();

        log.info("========== DataFrame 示例结束 ==========");
    }

    /**
     * Person 数据对象。
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Person {
        private String name;
        private int age;
        private String city;
    }
}
