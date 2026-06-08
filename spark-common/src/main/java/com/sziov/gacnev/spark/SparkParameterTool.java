package com.sziov.gacnev.spark;

import com.sziov.gacnev.constant.ParamsKeyConstant;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

/**
 * Spark 参数工具类，支持从多种数据源加载和合并配置。
 *
 * <p><b>支持的配置来源</b>：
 * <ul>
 *   <li>命令行参数 ({@code --key value} 或 {@code --key=value})</li>
 *   <li>外部 Properties 文件</li>
 *   <li>classpath 资源文件</li>
 *   <li>内存 Map</li>
 * </ul>
 *
 * @author maikou
 * @since 2026-05-16
 */
@Slf4j
public final class SparkParameterTool {

    /** 默认类路径配置文件名 */
    public static final String DEFAULT_CONFIG_FILE = "app.properties";

    private SparkParameterTool() {
        throw new UnsupportedOperationException("工具类不允许实例化");
    }

    // ==================== 加载 ====================

    /**
     * 从命令行参数创建 Properties。
     * <p>支持格式：{@code --key value} 或 {@code --key=value}
     *
     * @param args 命令行参数
     * @return Properties 对象（不为 null）
     */
    public static Properties fromArgs(String[] args) {
        Properties properties = new Properties();
        if (args == null || args.length == 0) {
            return properties;
        }

        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if (arg == null || !arg.startsWith("--")) {
                continue;
            }

            String key = arg.substring(2);
            String value;

            int eqIndex = key.indexOf('=');
            if (eqIndex > 0) {
                value = key.substring(eqIndex + 1);
                key = key.substring(0, eqIndex);
            } else if (i + 1 < args.length && args[i + 1] != null && !args[i + 1].startsWith("--")) {
                value = args[++i];
            } else {
                value = "";
            }

            if (StringUtils.isNotBlank(key)) {
                properties.setProperty(key.trim(), value != null ? value : "");
            }
        }

        return properties;
    }

    /**
     * 从外部 Properties 文件加载。
     *
     * @param filePath 文件路径
     * @return Properties 对象（不为 null）
     */
    public static Properties fromPropertiesFile(String filePath) {
        Properties properties = new Properties();
        if (StringUtils.isBlank(filePath)) {
            return properties;
        }

        File file = new File(filePath);
        if (!file.exists() || !file.isFile()) {
            log.warn("配置文件不存在: {}", filePath);
            return properties;
        }

        try (InputStream inputStream = Files.newInputStream(file.toPath())) {
            properties.load(inputStream);
            log.info("已加载配置文件: {} ({} 个配置项)", filePath, properties.size());
        } catch (IOException e) {
            log.error("加载配置文件失败: {}", filePath, e);
        }

        return properties;
    }

    /**
     * 从 classpath 加载 Properties。
     *
     * @param fileName 资源文件名
     * @return Properties 对象（不为 null）
     */
    public static Properties fromClasspath(String fileName) {
        Properties properties = new Properties();
        if (StringUtils.isBlank(fileName)) {
            return properties;
        }

        try (InputStream inputStream = SparkParameterTool.class.getClassLoader().getResourceAsStream(fileName)) {
            if (inputStream != null) {
                properties.load(inputStream);
                log.info("已从 classpath 加载配置: {} ({} 个配置项)", fileName, properties.size());
            } else {
                log.warn("classpath 中未找到配置文件: {}", fileName);
            }
        } catch (IOException e) {
            log.error("从 classpath 加载配置文件失败: {}", fileName, e);
        }

        return properties;
    }

    /**
     * 从 Map 创建 Properties。
     *
     * @param map 配置 Map
     * @return Properties 对象（不为 null）
     */
    public static Properties fromMap(Map<String, String> map) {
        Properties properties = new Properties();
        if (map == null || map.isEmpty()) {
            return properties;
        }
        map.forEach((key, value) -> {
            if (key != null) {
                properties.setProperty(key, value != null ? value : "");
            }
        });
        return properties;
    }

    // ==================== 合并 ====================

    /**
     * 合并多个 Properties 对象，后面的覆盖前面的。
     *
     * @param base  基础配置
     * @param overlays 覆盖配置（可变参数）
     * @return 合并后的 Properties
     */
    public static Properties merge(Properties base, Properties... overlays) {
        Properties merged = new Properties();
        if (base != null) {
            merged.putAll(base);
        }
        if (overlays != null) {
            for (Properties overlay : overlays) {
                if (overlay != null) {
                    merged.putAll(overlay);
                }
            }
        }
        return merged;
    }

    // ==================== 获取 ====================

    /**
     * 获取字符串参数。
     *
     * @param props        Properties
     * @param key          参数名
     * @param defaultValue 默认值
     * @return 参数值
     */
    public static String get(Properties props, String key, String defaultValue) {
        if (props == null || StringUtils.isBlank(key)) {
            return defaultValue;
        }
        return props.getProperty(key, defaultValue);
    }

    /**
     * 获取整数参数。
     *
     * @param props        Properties
     * @param key          参数名
     * @param defaultValue 默认值
     * @return 参数值
     */
    public static int getInt(Properties props, String key, int defaultValue) {
        String value = get(props, key, null);
        if (StringUtils.isBlank(value)) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            log.warn("参数 {} 的值 '{}' 不是有效的整数，使用默认值 {}", key, value, defaultValue);
            return defaultValue;
        }
    }

    /**
     * 获取长整数参数。
     *
     * @param props        Properties
     * @param key          参数名
     * @param defaultValue 默认值
     * @return 参数值
     */
    public static long getLong(Properties props, String key, long defaultValue) {
        String value = get(props, key, null);
        if (StringUtils.isBlank(value)) {
            return defaultValue;
        }
        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException e) {
            log.warn("参数 {} 的值 '{}' 不是有效的长整数，使用默认值 {}", key, value, defaultValue);
            return defaultValue;
        }
    }

    /**
     * 获取双精度浮点数参数。
     *
     * @param props        Properties
     * @param key          参数名
     * @param defaultValue 默认值
     * @return 参数值
     */
    public static double getDouble(Properties props, String key, double defaultValue) {
        String value = get(props, key, null);
        if (StringUtils.isBlank(value)) {
            return defaultValue;
        }
        try {
            return Double.parseDouble(value.trim());
        } catch (NumberFormatException e) {
            log.warn("参数 {} 的值 '{}' 不是有效的双精度浮点数，使用默认值 {}", key, value, defaultValue);
            return defaultValue;
        }
    }

    /**
     * 获取布尔参数。
     *
     * @param props        Properties
     * @param key          参数名
     * @param defaultValue 默认值
     * @return 参数值
     */
    public static boolean getBoolean(Properties props, String key, boolean defaultValue) {
        String value = get(props, key, null);
        if (StringUtils.isBlank(value)) {
            return defaultValue;
        }
        return Boolean.parseBoolean(value.trim());
    }

    /**
     * 获取所有参数 key。
     *
     * @param props Properties
     * @return key 集合
     */
    public static java.util.Set<String> getKeys(Properties props) {
        if (props == null) {
            return Collections.emptySet();
        }
        return props.stringPropertyNames();
    }

    /**
     * 从命令行参数中获取配置文件路径。
     *
     * @param args 命令行参数
     * @return 配置文件路径，不存在则返回 null
     */
    public static String getConfigFilePath(String[] args) {
        if (args == null || args.length == 0) {
            return null;
        }

        Properties argsProps = fromArgs(args);
        String[] configKeys = {ParamsKeyConstant.CONFIG_FILE, ParamsKeyConstant.CONFIG_FILE_ALT};
        for (String configKey : configKeys) {
            String configPath = argsProps.getProperty(configKey);
            if (StringUtils.isNotBlank(configPath)) {
                return configPath;
            }
        }

        return null;
    }
}
