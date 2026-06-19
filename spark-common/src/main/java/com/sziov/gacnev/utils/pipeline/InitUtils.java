package com.sziov.gacnev.utils.pipeline;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.stream.Collectors;

import com.sziov.gacnev.utils.spark.SparkSqlUtils;

import lombok.extern.slf4j.Slf4j;

import org.apache.spark.sql.SparkSession;

/**
 * 初始化工具：仅在本地模式 + --init 参数时建库建表。
 *
 * @author maikou
 * @since 2026-06-18
 */
@Slf4j
public final class InitUtils {

    private InitUtils() {}

    /**
     * 本地模式且参数含 --init 时执行建库建表，返回 true 让 main 退出；
     * 非本地模式打 WARN 并返回 false，生产环境不会误执行。
     *
     * @param spark     SparkSession
     * @param params    命令行参数
     * @param sqlPath   classpath SQL 脚本路径
     * @param databases 数据库名列表
     */
    public static boolean initIfNeeded(SparkSession spark, Properties params,
                                        String sqlPath, String... databases) {
        if (!params.containsKey("init")) {
            return false;
        }
        if (!spark.sparkContext().master().startsWith("local")) {
            log.warn("--init 仅在本地模式生效，当前 master={}，跳过初始化", spark.sparkContext().master());
            return false;
        }
        for (String db : databases) {
            SparkSqlUtils.createDatabase(spark, db);
        }
        try (InputStream is = InitUtils.class.getClassLoader().getResourceAsStream(sqlPath)) {
            if (is == null) {
                throw new IllegalStateException("未找到 " + sqlPath);
            }
            String content = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))
                    .lines()
                    .filter(line -> !line.trim().isEmpty() && !line.trim().startsWith("--"))
                    .collect(Collectors.joining("\n"));
            for (String stmt : content.split(";")) {
                String t = stmt.trim();
                if (!t.isEmpty()) {
                    SparkSqlUtils.executeUpdate(spark, t);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("初始化失败: " + sqlPath, e);
        }
        log.info("库表初始化完成");
        return true;
    }
}
