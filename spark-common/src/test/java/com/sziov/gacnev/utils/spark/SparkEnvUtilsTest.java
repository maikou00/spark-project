package com.sziov.gacnev.utils.spark;

import com.sziov.gacnev.AbstractSparkTest;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("SparkEnvUtils 环境工具测试")
class SparkEnvUtilsTest extends AbstractSparkTest {

    @Test
    @DisplayName("prepare_命令行参数_返回SparkSession")
    void prepare_commandLineArgs_returnsSparkSession() {
        SparkSession session = SparkEnvUtils.prepare(new String[]{"--app.name", "testApp", "--master", "local[1]"});
        assertThat(session).isNotNull();
        assertThat(session.sparkContext().isStopped()).isFalse();
        session.stop();
    }

    @Test
    @DisplayName("getAllConfigMsg_不抛异常")
    void getAllConfigMsg_validSpark_noException() {
        SparkEnvUtils.getAllConfigMsg(spark);
    }
}
