package com.sziov.gacnev.utils;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link JsonUtils} 单元测试。
 *
 * @author maikou
 * @since 2026-06-09
 */
@DisplayName("JsonUtils JSON工具测试")
class JsonUtilsTest {

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class TestBean {
        private String name;
        private int age;
    }

    // ==================== toJson / fromJson ====================

    @Test
    @DisplayName("toJson_普通对象_返回JSON字符串")
    void toJson_normalObject_returnsJsonString() {
        TestBean bean = new TestBean("张三", 25);
        String json = JsonUtils.toJson(bean);
        assertThat(json).contains("\"name\":\"张三\"");
        assertThat(json).contains("\"age\":25");
    }

    @Test
    @DisplayName("toJson_null_返回null")
    void toJson_null_returnsNull() {
        assertThat(JsonUtils.toJson(null)).isNull();
    }

    @Test
    @DisplayName("toJsonPretty_对象_返回格式化JSON")
    void toJsonPretty_object_returnsPrettyJson() {
        TestBean bean = new TestBean("张三", 25);
        String pretty = JsonUtils.toJsonPretty(bean);
        assertThat(pretty).contains("\n");
        assertThat(pretty).contains("\"name\"");
    }

    @Test
    @DisplayName("fromJson_合法JSON_反序列化成功")
    void fromJson_validJson_deserializesSuccessfully() {
        String json = "{\"name\":\"李四\",\"age\":30}";
        TestBean bean = JsonUtils.fromJson(json, TestBean.class);
        assertThat(bean).isNotNull();
        assertThat(bean.getName()).isEqualTo("李四");
        assertThat(bean.getAge()).isEqualTo(30);
    }

    @Test
    @DisplayName("fromJson_空字符串_返回null")
    void fromJson_emptyString_returnsNull() {
        assertThat(JsonUtils.fromJson("", TestBean.class)).isNull();
    }

    @Test
    @DisplayName("fromJson_非法JSON_返回null")
    void fromJson_invalidJson_returnsNull() {
        assertThat(JsonUtils.fromJson("{invalid}", TestBean.class)).isNull();
    }

    // ==================== fromJsonToList ====================

    @Test
    @DisplayName("fromJsonToList_JSON数组_返回List")
    void fromJsonToList_jsonArray_returnsList() {
        String json = "[{\"name\":\"A\",\"age\":1},{\"name\":\"B\",\"age\":2}]";
        List<TestBean> list = JsonUtils.fromJsonToList(json, TestBean.class);
        assertThat(list).hasSize(2);
        assertThat(list.get(0).getName()).isEqualTo("A");
        assertThat(list.get(1).getName()).isEqualTo("B");
    }

    @Test
    @DisplayName("fromJsonToList_空字符串_返回null")
    void fromJsonToList_emptyString_returnsNull() {
        assertThat(JsonUtils.fromJsonToList("", TestBean.class)).isNull();
    }

    // ==================== fromJsonToMap ====================

    @Test
    @DisplayName("fromJsonToMap_合法JSON_返回Map")
    void fromJsonToMap_validJson_returnsMap() {
        String json = "{\"key1\":\"value1\",\"key2\":123}";
        Map<String, Object> map = JsonUtils.fromJsonToMap(json);
        assertThat(map).isNotNull();
        assertThat(map.get("key1")).isEqualTo("value1");
        assertThat(map.get("key2")).isEqualTo(123);
    }

    @Test
    @DisplayName("fromJsonToMap_空字符串_返回null")
    void fromJsonToMap_emptyString_returnsNull() {
        assertThat(JsonUtils.fromJsonToMap("")).isNull();
    }

    // ==================== toMap / fromMap ====================

    @Test
    @DisplayName("toMap_对象_返回Map")
    void toMap_object_returnsMap() {
        TestBean bean = new TestBean("王五", 40);
        Map<String, Object> map = JsonUtils.toMap(bean);
        assertThat(map).isNotNull();
        assertThat(map.get("name")).isEqualTo("王五");
        assertThat(map.get("age")).isEqualTo(40);
    }

    @Test
    @DisplayName("toMap_null_返回null")
    void toMap_null_returnsNull() {
        assertThat(JsonUtils.toMap(null)).isNull();
    }

    @Test
    @DisplayName("fromMap_合法Map_返回对象")
    void fromMap_validMap_returnsObject() {
        Map<String, Object> map = new HashMap<>();
        map.put("name", "赵六");
        map.put("age", 50);
        TestBean bean = JsonUtils.fromMap(map, TestBean.class);
        assertThat(bean).isNotNull();
        assertThat(bean.getName()).isEqualTo("赵六");
        assertThat(bean.getAge()).isEqualTo(50);
    }

    // ==================== isValidJson ====================

    @Test
    @DisplayName("isValidJson_合法JSON_返回true")
    void isValidJson_validJson_returnsTrue() {
        assertThat(JsonUtils.isValidJson("{\"a\":1}")).isTrue();
        assertThat(JsonUtils.isValidJson("[1,2,3]")).isTrue();
        assertThat(JsonUtils.isValidJson("\"hello\"")).isTrue();
        assertThat(JsonUtils.isValidJson("123")).isTrue();
    }

    @Test
    @DisplayName("isValidJson_非法JSON_返回false")
    void isValidJson_invalidJson_returnsFalse() {
        assertThat(JsonUtils.isValidJson("{bad}")).isFalse();
        assertThat(JsonUtils.isValidJson("")).isFalse();
        assertThat(JsonUtils.isValidJson("not json")).isFalse();
    }
}
