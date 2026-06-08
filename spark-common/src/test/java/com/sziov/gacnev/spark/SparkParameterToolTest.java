package com.sziov.gacnev.spark;

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

/**
 * {@link SparkParameterTool} 测试用例。
 *
 * @author maikou
 * @since 2026-06-09
 */
@DisplayName("SparkParameterTool 参数工具测试")
class SparkParameterToolTest {

    @Test
    @DisplayName("fromArgs_带键值对参数_解析为Properties")
    void fromArgs_keyValuePairs_parsesToProperties() {
        Properties props = SparkParameterTool.fromArgs(new String[]{"--app.name", "testApp", "--master", "local"});
        assertThat(props).containsEntry("app.name", "testApp").containsEntry("master", "local");
    }

    @Test
    @DisplayName("fromArgs_空参数_返回空Properties")
    void fromArgs_emptyArgs_returnsEmpty() {
        assertThat(SparkParameterTool.fromArgs(new String[0])).isEmpty();
        assertThat(SparkParameterTool.fromArgs(null)).isEmpty();
    }

    @Test
    @DisplayName("fromArgs_奇数个参数_多余参数值为空字符串")
    void fromArgs_oddNumberOfArgs_ignoresLast() {
        Properties props = SparkParameterTool.fromArgs(new String[]{"--key1", "val1", "--key2"});
        assertThat(props).containsEntry("key1", "val1");
        assertThat(props).containsEntry("key2", "");
    }

    @Test
    @DisplayName("fromPropertiesFile_有效文件_加载Properties")
    void fromPropertiesFile_validFile_loadsProperties(@TempDir Path tempDir) throws IOException {
        File propFile = tempDir.resolve("test.properties").toFile();
        try (FileOutputStream fos = new FileOutputStream(propFile)) {
            Properties source = new Properties();
            source.setProperty("key1", "value1");
            source.setProperty("key2", "value2");
            source.store(fos, null);
        }
        Properties props = SparkParameterTool.fromPropertiesFile(propFile.getAbsolutePath());
        assertThat(props).hasSize(2).containsEntry("key1", "value1");
    }

    @Test
    @DisplayName("fromPropertiesFile_不存在文件_返回空")
    void fromPropertiesFile_nonExistentFile_returnsEmpty() {
        Properties props = SparkParameterTool.fromPropertiesFile("/nonexistent/file.properties");
        assertThat(props).isEmpty();
    }

    @Test
    @DisplayName("fromMap_有效Map_转为Properties")
    void fromMap_validMap_convertsToProperties() {
        Map<String, String> map = new HashMap<>();
        map.put("key1", "value1");
        map.put("key2", "value2");
        Properties props = SparkParameterTool.fromMap(map);
        assertThat(props).hasSize(2).containsEntry("key1", "value1");
    }

    @Test
    @DisplayName("fromMap_null或空Map_返回空Properties")
    void fromMap_nullOrEmpty_returnsEmpty() {
        assertThat(SparkParameterTool.fromMap(null)).isEmpty();
        assertThat(SparkParameterTool.fromMap(new HashMap<>())).isEmpty();
    }

    @Test
    @DisplayName("merge_两个Properties_覆盖合并")
    void merge_twoProperties_overlayMerges() {
        Properties base = new Properties();
        base.setProperty("key1", "base1");
        base.setProperty("key2", "base2");
        Properties overlay = new Properties();
        overlay.setProperty("key2", "overlay2");
        overlay.setProperty("key3", "overlay3");
        Properties merged = SparkParameterTool.merge(base, overlay);
        assertThat(merged).containsEntry("key1", "base1")
                .containsEntry("key2", "overlay2")
                .containsEntry("key3", "overlay3");
    }

    @Test
    @DisplayName("get_有效key_返回对应值")
    void get_validKey_returnsValue() {
        Properties props = new Properties();
        props.setProperty("key", "value");
        assertThat(SparkParameterTool.get(props, "key", "default")).isEqualTo("value");
    }

    @Test
    @DisplayName("get_不存在key_返回默认值")
    void get_nonExistentKey_returnsDefault() {
        Properties props = new Properties();
        assertThat(SparkParameterTool.get(props, "nonexistent", "default")).isEqualTo("default");
        assertThat(SparkParameterTool.get(null, "key", "default")).isEqualTo("default");
    }

    @Test
    @DisplayName("getInt_有效整数_正确解析")
    void getInt_validInteger_parsesCorrectly() {
        Properties props = new Properties();
        props.setProperty("key", "123");
        assertThat(SparkParameterTool.getInt(props, "key", 0)).isEqualTo(123);
    }

    @Test
    @DisplayName("getInt_不存在key_返回默认值")
    void getInt_nonExistentKey_returnsDefault() {
        Properties props = new Properties();
        assertThat(SparkParameterTool.getInt(props, "nonexistent", 42)).isEqualTo(42);
    }

    @Test
    @DisplayName("getInt_无效数值_返回默认值")
    void getInt_invalidValue_returnsDefault() {
        Properties props = new Properties();
        props.setProperty("key", "not-a-number");
        assertThat(SparkParameterTool.getInt(props, "key", 42)).isEqualTo(42);
    }

    @Test
    @DisplayName("getBoolean_true值_返回true")
    void getBoolean_trueValue_returnsTrue() {
        Properties props = new Properties();
        props.setProperty("k1", "true");
        props.setProperty("k2", "false");
        assertThat(SparkParameterTool.getBoolean(props, "k1", false)).isTrue();
        assertThat(SparkParameterTool.getBoolean(props, "k2", true)).isFalse();
        assertThat(SparkParameterTool.getBoolean(props, "nonexistent", true)).isTrue();
    }

    @Test
    @DisplayName("getLong_有效长整数_正确解析")
    void getLong_validLong_parsesCorrectly() {
        Properties props = new Properties();
        props.setProperty("key", "10000000000");
        assertThat(SparkParameterTool.getLong(props, "key", 0L)).isEqualTo(10000000000L);
    }

    @Test
    @DisplayName("getDouble_有效浮点数_正确解析")
    void getDouble_validDouble_parsesCorrectly() {
        Properties props = new Properties();
        props.setProperty("key", "3.14");
        assertThat(SparkParameterTool.getDouble(props, "key", 0.0))
                .isCloseTo(3.14, org.assertj.core.data.Offset.offset(0.001));
    }

    @Test
    @DisplayName("getConfigFilePath_含config参数_返回路径")
    void getConfigFilePath_withConfigArg_returnsPath() {
        String[] args = {"--config", "/path/to/config.properties", "--app.name", "test"};
        assertThat(SparkParameterTool.getConfigFilePath(args)).isEqualTo("/path/to/config.properties");
    }

    @Test
    @DisplayName("getConfigFilePath_无config参数_返回null")
    void getConfigFilePath_noConfigArg_returnsNull() {
        assertThat(SparkParameterTool.getConfigFilePath(null)).isNull();
        assertThat(SparkParameterTool.getConfigFilePath(new String[]{"--app.name", "test"})).isNull();
    }
}
