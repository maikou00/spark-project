package com.sziov.gacnev.common;

import com.fasterxml.jackson.core.type.TypeReference;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("JsonUtils JSON工具测试")
class JsonUtilsTest {

    @Test
    @DisplayName("toJson_普通对象_返回JSON字符串")
    void toJson_plainObject_returnsJsonString() {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("name", "张三");
        map.put("age", 25);
        String json = JsonUtils.toJson(map);
        assertThat(json).contains("\"name\"").contains("\"age\"");
    }

    @Test
    @DisplayName("toJson_null输入_返回null")
    void toJson_nullInput_returnsNull() {
        assertThat(JsonUtils.toJson(null)).isNull();
    }

    @Test
    @DisplayName("fromJson_有效JSON_解析为Map")
    void fromJson_validJson_parsesToMap() {
        Map<String, Object> map = JsonUtils.fromJson("{\"name\":\"张三\",\"age\":25}", Map.class);
        assertThat(map).isNotNull();
        assertThat(map.get("name")).isEqualTo("张三");
        assertThat(map.get("age")).isEqualTo(25);
    }

    @Test
    @DisplayName("fromJson_带TypeReference_正确解析泛型")
    void fromJson_withTypeReference_parsesGenericType() {
        List<Map<String, Object>> list = JsonUtils.fromJson(
                "[{\"k\":\"v1\"},{\"k\":\"v2\"}]",
                new TypeReference<List<Map<String, Object>>>() {});
        assertThat(list).hasSize(2);
        assertThat(list.get(0).get("k")).isEqualTo("v1");
    }

    @Test
    @DisplayName("toJsonPretty_对象_返回格式化JSON")
    void toJsonPretty_object_returnsPrettyPrinted() {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("name", "test");
        String pretty = JsonUtils.toJsonPretty(map);
        assertThat(pretty).contains("\n");
    }

    @Test
    @DisplayName("isValidJson_有效JSON_返回true")
    void isValidJson_validJson_returnsTrue() {
        assertThat(JsonUtils.isValidJson("{\"key\":\"value\"}")).isTrue();
        assertThat(JsonUtils.isValidJson("[1,2,3]")).isTrue();
    }

    @Test
    @DisplayName("isValidJson_无效JSON_返回false")
    void isValidJson_invalidJson_returnsFalse() {
        assertThat(JsonUtils.isValidJson("{invalid}")).isFalse();
        assertThat(JsonUtils.isValidJson(null)).isFalse();
    }
}
