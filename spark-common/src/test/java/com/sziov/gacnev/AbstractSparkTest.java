package com.sziov.gacnev;

import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

/**
 * Spark 测试基类，统一管理 {@link SparkSession} 生命周期。
 *
 * @author maikou
 * @since 2026-06-09
 */
public abstract class AbstractSparkTest {

    protected static SparkSession spark;

    @BeforeAll
    static void initSpark() {
        spark = SparkSession.builder()
                .appName("test")
                .master("local[1]")
                .config("spark.ui.enabled", "false")
                .config("spark.sql.warehouse.dir",
                        System.getProperty("java.io.tmpdir") + "/spark-warehouse-test")
                .config("spark.sql.catalogImplementation", "in-memory")
                .config("spark.sql.shuffle.partitions", "1")
                .getOrCreate();
    }

    @AfterAll
    static void stopSpark() {
        if (spark != null) {
            spark.stop();
            spark = null;
        }
    }
}
