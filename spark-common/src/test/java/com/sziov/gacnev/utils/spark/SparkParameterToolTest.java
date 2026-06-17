package com.sziov.gacnev.utils.spark;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link SparkParameterTool} 单元测试。
 *
 * @author maikou
 * @since 2026-06-09
 */
@DisplayName("SparkParameterTool 参数工具测试")
class SparkParameterToolTest {

    // ==================== fromArgs ====================

    @Test
    @DisplayName("fromArgs_空参数_返回空Properties")
    void fromArgs_emptyArgs_returnsEmptyProperties() {
        Properties props = SparkParameterTool.fromArgs(new String[]{});
        assertThat(props).isEmpty();
    }

    @Test
    @DisplayName("fromArgs_null参数_返回空Properties")
    void fromArgs_nullArgs_returnsEmptyProperties() {
        Properties props = SparkParameterTool.fromArgs(null);
        assertThat(props).isEmpty();
    }

    @Test
    @DisplayName("fromArgs_--key_value格式_正确解析")
    void fromArgs_keyValueFormat_parsesCorrectly() {
        Properties props = SparkParameterTool.fromArgs(new String[]{"--app.name", "MyApp", "--mode", "prod"});
        assertThat(props.getProperty("app.name")).isEqualTo("MyApp");
        assertThat(props.getProperty("mode")).isEqualTo("prod");
    }

    @Test
    @DisplayName("fromArgs_--key=value格式_正确解析")
    void fromArgs_keyEqualsValueFormat_parsesCorrectly() {
        Properties props = SparkParameterTool.fromArgs(new String[]{"--app.name=MyApp", "--mode=prod"});
        assertThat(props.getProperty("app.name")).isEqualTo("MyApp");
        assertThat(props.getProperty("mode")).isEqualTo("prod");
    }

    @Test
    @DisplayName("fromArgs_混合格式_正确解析")
    void fromArgs_mixedFormats_parsesCorrectly() {
        Properties props = SparkParameterTool.fromArgs(new String[]{"--name", "test", "--debug=true"});
        assertThat(props.getProperty("name")).isEqualTo("test");
        assertThat(props.getProperty("debug")).isEqualTo("true");
    }

    // ==================== fromMap ====================

    @Test
    @DisplayName("fromMap_普通Map_正确转换为Properties")
    void fromMap_normalMap_convertsToProperties() {
        Map<String, String> map = new HashMap<>();
        map.put("key1", "value1");
        map.put("key2", "value2");

        Properties props = SparkParameterTool.fromMap(map);
        assertThat(props.getProperty("key1")).isEqualTo("value1");
        assertThat(props.getProperty("key2")).isEqualTo("value2");
    }

    @Test
    @DisplayName("fromMap_nullMap_返回空Properties")
    void fromMap_nullMap_returnsEmptyProperties() {
        Properties props = SparkParameterTool.fromMap(null);
        assertThat(props).isEmpty();
    }

    @Test
    @DisplayName("fromMap_空Map_返回空Properties")
    void fromMap_emptyMap_returnsEmptyProperties() {
        Properties props = SparkParameterTool.fromMap(new HashMap<>());
        assertThat(props).isEmpty();
    }

    // ==================== merge ====================

    @Test
    @DisplayName("merge_后面覆盖前面_覆盖生效")
    void merge_overlayOverrides_correctResult() {
        Properties base = new Properties();
        base.setProperty("a", "1");
        base.setProperty("b", "2");

        Properties overlay = new Properties();
        overlay.setProperty("b", "3");
        overlay.setProperty("c", "4");

        Properties merged = SparkParameterTool.merge(base, overlay);
        assertThat(merged.getProperty("a")).isEqualTo("1");  // base 保留
        assertThat(merged.getProperty("b")).isEqualTo("3");  // overlay 覆盖
        assertThat(merged.getProperty("c")).isEqualTo("4");  // overlay 新增
    }

    @Test
    @DisplayName("merge_base为null_仅返回覆盖属性")
    void merge_nullBase_returnsOverlay() {
        Properties overlay = new Properties();
        overlay.setProperty("x", "y");
        Properties merged = SparkParameterTool.merge(null, overlay);
        assertThat(merged.getProperty("x")).isEqualTo("y");
    }

    // ==================== get ====================

    @Test
    @DisplayName("get_key存在_返回对应值")
    void get_keyExists_returnsValue() {
        Properties props = new Properties();
        props.setProperty("name", "test");
        assertThat(SparkParameterTool.get(props, "name", "default")).isEqualTo("test");
    }

    @Test
    @DisplayName("get_key不存在_返回默认值")
    void get_keyNotExists_returnsDefault() {
        Properties props = new Properties();
        assertThat(SparkParameterTool.get(props, "missing", "default")).isEqualTo("default");
    }

    @Test
    @DisplayName("get_props为null_返回默认值")
    void get_nullProps_returnsDefault() {
        assertThat(SparkParameterTool.get(null, "key", "default")).isEqualTo("default");
    }

    // ==================== getInt ====================

    @Test
    @DisplayName("getInt_有效整数值_正确解析")
    void getInt_validValue_returnsParsedInt() {
        Properties props = new Properties();
        props.setProperty("count", "10");
        assertThat(SparkParameterTool.getInt(props, "count", 0)).isEqualTo(10);
    }

    @Test
    @DisplayName("getInt_无效值_返回默认值")
    void getInt_invalidValue_returnsDefault() {
        Properties props = new Properties();
        props.setProperty("count", "not_a_number");
        assertThat(SparkParameterTool.getInt(props, "count", 5)).isEqualTo(5);
    }

    @Test
    @DisplayName("getInt_空值_返回默认值")
    void getInt_emptyValue_returnsDefault() {
        Properties props = new Properties();
        props.setProperty("count", "");
        assertThat(SparkParameterTool.getInt(props, "count", 5)).isEqualTo(5);
    }

    // ==================== getBoolean ====================

    @Test
    @DisplayName("getBoolean_true字符串_返回true")
    void getBoolean_trueString_returnsTrue() {
        Properties props = new Properties();
        props.setProperty("enabled", "true");
        assertThat(SparkParameterTool.getBoolean(props, "enabled", false)).isTrue();
    }

    @Test
    @DisplayName("getBoolean_false字符串_返回false")
    void getBoolean_falseString_returnsFalse() {
        Properties props = new Properties();
        props.setProperty("enabled", "false");
        assertThat(SparkParameterTool.getBoolean(props, "enabled", true)).isFalse();
    }

    @Test
    @DisplayName("getBoolean_不存在_返回默认值")
    void getBoolean_notExists_returnsDefault() {
        Properties props = new Properties();
        assertThat(SparkParameterTool.getBoolean(props, "enabled", true)).isTrue();
    }

    // ==================== getLong / getDouble ====================

    @Test
    @DisplayName("getLong_有效值_正确解析")
    void getLong_validValue_returnsParsedLong() {
        Properties props = new Properties();
        props.setProperty("big", String.valueOf(Long.MAX_VALUE));
        assertThat(SparkParameterTool.getLong(props, "big", 0L)).isEqualTo(Long.MAX_VALUE);
    }

    @Test
    @DisplayName("getDouble_有效值_正确解析")
    void getDouble_validValue_returnsParsedDouble() {
        Properties props = new Properties();
        props.setProperty("ratio", "0.75");
        assertThat(SparkParameterTool.getDouble(props, "ratio", 0.0)).isEqualTo(0.75);
    }

    // ==================== getConfigFilePath ====================

    @Test
    @DisplayName("getConfigFilePath_args包含config参数_返回路径")
    void getConfigFilePath_configArg_returnsPath() {
        String path = SparkParameterTool.getConfigFilePath(new String[]{"--config", "/path/to/app.properties"});
        assertThat(path).isEqualTo("/path/to/app.properties");
    }

    @Test
    @DisplayName("getConfigFilePath_args包含config-file参数_返回路径")
    void getConfigFilePath_configFileArg_returnsPath() {
        String path = SparkParameterTool.getConfigFilePath(new String[]{"--config-file", "/other/path.properties"});
        assertThat(path).isEqualTo("/other/path.properties");
    }

    @Test
    @DisplayName("getConfigFilePath_args无config参数_返回null")
    void getConfigFilePath_noConfigArg_returnsNull() {
        String path = SparkParameterTool.getConfigFilePath(new String[]{"--app.name", "test"});
        assertThat(path).isNull();
    }

    @Test
    @DisplayName("getConfigFilePath_nullArgs_返回null")
    void getConfigFilePath_nullArgs_returnsNull() {
        String path = SparkParameterTool.getConfigFilePath(null);
        assertThat(path).isNull();
    }

    // ==================== fromClasspath ====================

    @Test
    @DisplayName("fromClasspath_存在的配置文件_成功加载")
    void fromClasspath_existingFile_loadsSuccessfully() {
        Properties props = SparkParameterTool.fromClasspath("app.properties");
        assertThat(props).isNotNull();
        assertThat(props.size()).isGreaterThan(0);
    }

    @Test
    @DisplayName("fromClasspath_不存在文件_返回空Properties")
    void fromClasspath_nonExistingFile_returnsEmptyProperties() {
        Properties props = SparkParameterTool.fromClasspath("nonexistent.properties");
        assertThat(props).isEmpty();
    }
}
