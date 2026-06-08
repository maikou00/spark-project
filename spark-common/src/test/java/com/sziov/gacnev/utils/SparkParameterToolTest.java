package com.sziov.gacnev.utils;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

/**
 * {@link SparkParameterTool} 测试用例。
 */
@DisplayName("SparkParameterTool 测试")
class SparkParameterToolTest {

    @Test
    @DisplayName("从空参数创建 Properties 应返回空")
    void fromArgsWithNullShouldReturnEmpty() {
        Properties props = SparkParameterTool.fromArgs(null);
        assertThat(props).isEmpty();
    }

    @Test
    @DisplayName("从空数组创建 Properties 应返回空")
    void fromArgsWithEmptyArrayShouldReturnEmpty() {
        Properties props = SparkParameterTool.fromArgs(new String[]{});
        assertThat(props).isEmpty();
    }

    @Test
    @DisplayName("从 --key value 格式参数创建 Properties")
    void fromArgsWithSeparateKeyValue() {
        Properties props = SparkParameterTool.fromArgs(new String[]{
                "--app.name", "MyApp",
                "--spark.local", "true"
        });
        assertThat(props)
                .hasSize(2)
                .containsEntry("app.name", "MyApp")
                .containsEntry("spark.local", "true");
    }

    @Test
    @DisplayName("从 --key=value 格式参数创建 Properties")
    void fromArgsWithEqualsFormat() {
        Properties props = SparkParameterTool.fromArgs(new String[]{
                "--app.name=MyApp",
                "--spark.local=false"
        });
        assertThat(props)
                .hasSize(2)
                .containsEntry("app.name", "MyApp")
                .containsEntry("spark.local", "false");
    }

    @Test
    @DisplayName("混合格式参数")
    void fromArgsWithMixedFormat() {
        Properties props = SparkParameterTool.fromArgs(new String[]{
                "--app.name=MyApp",
                "--spark.local",
                "true",
                "--config", "/path/to/config.properties"
        });
        assertThat(props)
                .hasSize(3)
                .containsEntry("config", "/path/to/config.properties");
    }

    @Test
    @DisplayName("从文件加载 Properties")
    void fromPropertiesFile(@TempDir Path tempDir) throws IOException {
        File propFile = tempDir.resolve("test.properties").toFile();
        try (FileOutputStream fos = new FileOutputStream(propFile)) {
            Properties source = new Properties();
            source.setProperty("key1", "value1");
            source.setProperty("key2", "value2");
            source.store(fos, null);
        }

        Properties props = SparkParameterTool.fromPropertiesFile(propFile.getAbsolutePath());
        assertThat(props)
                .hasSize(2)
                .containsEntry("key1", "value1")
                .containsEntry("key2", "value2");
    }

    @Test
    @DisplayName("从不存在文件加载应返回空")
    void fromPropertiesFileWithNonExistentFile() {
        Properties props = SparkParameterTool.fromPropertiesFile("/nonexistent/file.properties");
        assertThat(props).isEmpty();
    }

    @Test
    @DisplayName("从 Map 创建 Properties")
    void fromMap() {
        Map<String, String> map = new HashMap<>();
        map.put("key1", "value1");
        map.put("key2", "value2");

        Properties props = SparkParameterTool.fromMap(map);
        assertThat(props)
                .hasSize(2)
                .containsEntry("key1", "value1");
    }

    @Test
    @DisplayName("从空 Map 创建应返回空")
    void fromMapWithEmpty() {
        assertThat(SparkParameterTool.fromMap(null)).isEmpty();
        assertThat(SparkParameterTool.fromMap(new HashMap<>())).isEmpty();
    }

    @Test
    @DisplayName("合并 Properties")
    void merge() {
        Properties base = new Properties();
        base.setProperty("key1", "base1");
        base.setProperty("key2", "base2");

        Properties overlay = new Properties();
        overlay.setProperty("key2", "overlay2");
        overlay.setProperty("key3", "overlay3");

        Properties merged = SparkParameterTool.merge(base, overlay);
        assertThat(merged)
                .containsEntry("key1", "base1")
                .containsEntry("key2", "overlay2")  // 被覆盖
                .containsEntry("key3", "overlay3");
    }

    @Test
    @DisplayName("获取配置值-字符串")
    void getString() {
        Properties props = new Properties();
        props.setProperty("key", "value");

        assertThat(SparkParameterTool.get(props, "key", "default")).isEqualTo("value");
        assertThat(SparkParameterTool.get(props, "nonexistent", "default")).isEqualTo("default");
        assertThat(SparkParameterTool.get(null, "key", "default")).isEqualTo("default");
    }

    @Test
    @DisplayName("获取配置值-整数")
    void getInt() {
        Properties props = new Properties();
        props.setProperty("key", "123");

        assertThat(SparkParameterTool.getInt(props, "key", 0)).isEqualTo(123);
        assertThat(SparkParameterTool.getInt(props, "nonexistent", 42)).isEqualTo(42);
        assertThat(SparkParameterTool.getInt(props, "key", 0)).isEqualTo(123);
    }

    @Test
    @DisplayName("获取配置值-无效整数应返回默认值")
    void getIntWithInvalidValue() {
        Properties props = new Properties();
        props.setProperty("key", "not-a-number");

        assertThat(SparkParameterTool.getInt(props, "key", 42)).isEqualTo(42);
    }

    @Test
    @DisplayName("获取配置值-布尔")
    void getBoolean() {
        Properties props = new Properties();
        props.setProperty("k1", "true");
        props.setProperty("k2", "false");

        assertThat(SparkParameterTool.getBoolean(props, "k1", false)).isTrue();
        assertThat(SparkParameterTool.getBoolean(props, "k2", true)).isFalse();
        assertThat(SparkParameterTool.getBoolean(props, "nonexistent", true)).isTrue();
    }

    @Test
    @DisplayName("获取配置值-长整数")
    void getLong() {
        Properties props = new Properties();
        props.setProperty("key", "10000000000");

        assertThat(SparkParameterTool.getLong(props, "key", 0L)).isEqualTo(10000000000L);
    }

    @Test
    @DisplayName("获取配置值-双精度浮点数")
    void getDouble() {
        Properties props = new Properties();
        props.setProperty("key", "3.14");

        assertThat(SparkParameterTool.getDouble(props, "key", 0.0)).isCloseTo(3.14, org.assertj.core.data.Offset.offset(0.001));
    }

    @Test
    @DisplayName("从命令行获取配置文件路径")
    void getConfigFilePath() {
        String[] args = {"--config", "/path/to/config.properties", "--app.name", "test"};
        assertThat(SparkParameterTool.getConfigFilePath(args)).isEqualTo("/path/to/config.properties");
    }

    @Test
    @DisplayName("无配置文件参数时应返回 null")
    void getConfigFilePathWithNoConfig() {
        assertThat(SparkParameterTool.getConfigFilePath(null)).isNull();
        assertThat(SparkParameterTool.getConfigFilePath(new String[]{"--app.name", "test"})).isNull();
    }
}
