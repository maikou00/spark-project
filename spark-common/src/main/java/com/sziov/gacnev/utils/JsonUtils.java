package com.sziov.gacnev.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * JSON工具类
 * 提供JSON序列化、反序列化等功能
 *
 * @author maikou
 * @since 2026-05-17
 */
@Slf4j
public final class JsonUtils {

    /**
     * ObjectMapper实例（线程安全）
     */
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    static {
        // 忽略未知属性
        OBJECT_MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        // 忽略空对象
        OBJECT_MAPPER.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
    }

    private JsonUtils() {}

    /**
     * 对象转JSON字符串
     *
     * @param obj 对象
     * @return JSON字符串
     */
    public static String toJson(Object obj) {
        if (Objects.isNull(obj)) {
            return null;
        }
        try {
            return OBJECT_MAPPER.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize object to JSON: {}", obj, e);
            return null;
        }
    }

    /**
     * 对象转JSON字符串（格式化）
     *
     * @param obj 对象
     * @return JSON字符串（格式化）
     */
    public static String toJsonPretty(Object obj) {
        if (Objects.isNull(obj)) {
            return null;
        }
        try {
            return OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize object to pretty JSON: {}", obj, e);
            return null;
        }
    }

    /**
     * JSON字符串转对象
     *
     * @param json  JSON字符串
     * @param clazz 目标类型
     * @param <T>   泛型
     * @return 对象
     */
    public static <T> T fromJson(String json, Class<T> clazz) {
        if (StringUtils.isBlank(json) || Objects.isNull(clazz)) {
            return null;
        }
        try {
            return OBJECT_MAPPER.readValue(json, clazz);
        } catch (JsonProcessingException e) {
            log.error("Failed to deserialize JSON to object: {}, class: {}", json, clazz.getName(), e);
            return null;
        }
    }

    /**
     * JSON字符串转对象（支持泛型）
     *
     * @param json          JSON字符串
     * @param typeReference 类型引用
     * @param <T>           泛型
     * @return 对象
     */
    public static <T> T fromJson(String json, TypeReference<T> typeReference) {
        if (StringUtils.isBlank(json) || Objects.isNull(typeReference)) {
            return null;
        }
        try {
            return OBJECT_MAPPER.readValue(json, typeReference);
        } catch (JsonProcessingException e) {
            log.error("Failed to deserialize JSON to object: {}, type: {}", json, typeReference.getType(), e);
            return null;
        }
    }

    /**
     * JSON字符串转List
     *
     * @param json  JSON字符串
     * @param clazz 元素类型
     * @param <T>   泛型
     * @return List对象
     */
    public static <T> List<T> fromJsonToList(String json, Class<T> clazz) {
        if (StringUtils.isBlank(json) || Objects.isNull(clazz)) {
            return null;
        }
        try {
            return OBJECT_MAPPER.readValue(json, OBJECT_MAPPER.getTypeFactory().constructCollectionType(List.class, clazz));
        } catch (JsonProcessingException e) {
            log.error("Failed to deserialize JSON to List: {}, class: {}", json, clazz.getName(), e);
            return null;
        }
    }

    /**
     * JSON字符串转Map
     *
     * @param json JSON字符串
     * @return Map对象
     */
    public static Map<String, Object> fromJsonToMap(String json) {
        if (StringUtils.isBlank(json)) {
            return null;
        }
        try {
            return OBJECT_MAPPER.readValue(json, new TypeReference<Map<String, Object>>() {});
        } catch (JsonProcessingException e) {
            log.error("Failed to deserialize JSON to Map: {}", json, e);
            return null;
        }
    }

    /**
     * 对象转Map
     *
     * @param obj 对象
     * @return Map对象
     */
    public static Map<String, Object> toMap(Object obj) {
        if (Objects.isNull(obj)) {
            return null;
        }
        try {
            return OBJECT_MAPPER.convertValue(obj, new TypeReference<Map<String, Object>>() {});
        } catch (RuntimeException e) {
            log.error("Failed to convert object to Map: {}", obj, e);
            return null;
        }
    }

    /**
     * Map转对象
     *
     * @param map   Map对象
     * @param clazz 目标类型
     * @param <T>   泛型
     * @return 对象
     */
    public static <T> T fromMap(Map<String, Object> map, Class<T> clazz) {
        if (Objects.isNull(map) || Objects.isNull(clazz)) {
            return null;
        }
        try {
            return OBJECT_MAPPER.convertValue(map, clazz);
        } catch (RuntimeException e) {
            log.error("Failed to convert Map to object: {}, class: {}", map, clazz.getName(), e);
            return null;
        }
    }

    /**
     * 判断字符串是否为合法JSON
     *
     * @param json JSON字符串
     * @return 是否合法
     */
    public static boolean isValidJson(String json) {
        if (StringUtils.isBlank(json)) {
            return false;
        }
        try {
            OBJECT_MAPPER.readTree(json);
            return true;
        } catch (JsonProcessingException e) {
            return false;
        }
    }
}
