package com.sziov.gacnev.utils.etl;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.length;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.substring;
import static org.apache.spark.sql.functions.trim;
import static org.apache.spark.sql.functions.when;


/**
 * ETL数据处理工具类
 * <p>提供通用的数据清洗、转换、过滤功能</p>
 *
 * @author maikou
 * @since 2026-05-18
 */
@Slf4j
public final class EtlUtils {

    private static final String EMPTY_STRING = "";

    private EtlUtils() {
        throw new UnsupportedOperationException("工具类不允许实例化");
    }

    /**
     * 数据清洗：去除字符串字段的首尾空格，空字符串转为null
     *
     * @param df 待清洗的Dataset
     * @return 清洗后的Dataset
     */
    public static Dataset<Row> cleanData(Dataset<Row> df) {
        Objects.requireNonNull(df, "Dataset cannot be null");

        String[] columns = df.columns();
        Dataset<Row> result = df;

        for (String colName : columns) {
            if (result.schema().apply(colName).dataType().equals(DataTypes.StringType)) {
                result = result.withColumn(colName, trim(col(colName)))
                        .withColumn(colName, functions.when(col(colName).equalTo(EMPTY_STRING), null)
                                .otherwise(col(colName)));
            }
        }
        return result;
    }

    /**
     * 数据清洗：去除指定字符串字段的首尾空格，空字符串转为null
     *
     * @param df        待清洗的Dataset
     * @param colNames  需要清洗的列名数组
     * @return 清洗后的Dataset
     */
    public static Dataset<Row> cleanData(Dataset<Row> df, String[] colNames) {
        Dataset<Row> result = df;
        for (String colName : colNames) {
            if (Arrays.asList(df.columns()).contains(colName)) {
                if (result.schema().apply(colName).dataType().equals(DataTypes.StringType)) {
                    result = result.withColumn(colName, trim(col(colName)))
                            .withColumn(colName, functions.when(col(colName).equalTo(EMPTY_STRING), null)
                                    .otherwise(col(colName)));
                }
            }
        }
        return result;
    }

    /**
     * 数据过滤：过滤指定字段为空的数据
     *
     * @param df        待过滤的Dataset
     * @param colNames  需要校验非空的列名数组
     * @return 过滤后的Dataset
     */
    public static Dataset<Row> filterNotNull(Dataset<Row> df, String[] colNames) {
        Dataset<Row> result = df;
        for (String colName : colNames) {
            if (Arrays.asList(df.columns()).contains(colName)) {
                result = result.filter(col(colName).isNotNull().and(col(colName).notEqual(EMPTY_STRING)));
            }
        }
        return result;
    }

    /**
     * 数据过滤：过滤指定字段为空的数据（单个字段）
     *
     * @param df       待过滤的Dataset
     * @param colName  需要校验非空的列名
     * @return 过滤后的Dataset
     */
    public static Dataset<Row> filterNotNull(Dataset<Row> df, String colName) {
        return filterNotNull(df, new String[]{colName});
    }

    /**
     * 添加处理时间戳字段
     *
     * @param df             待处理的Dataset
     * @param timestampColName 时间戳字段名
     * @return 添加时间戳后的Dataset
     */
    public static Dataset<Row> addProcessTimestamp(Dataset<Row> df, String timestampColName) {
        return df.withColumn(timestampColName, functions.current_timestamp());
    }

    /**
     * 添加处理时间戳字段（使用默认字段名process_time）
     *
     * @param df 待处理的Dataset
     * @return 添加时间戳后的Dataset
     */
    public static Dataset<Row> addProcessTimestamp(Dataset<Row> df) {
        return addProcessTimestamp(df, "process_time");
    }

    /**
     * 邮箱脱敏（保留前3位和@后的域名）
     *
     * @param df             待处理的Dataset
     * @param emailColName   邮箱字段名
     * @param maskedColName  脱敏后的字段名
     * @return 添加脱敏字段后的Dataset
     */
    public static Dataset<Row> maskEmail(Dataset<Row> df, String emailColName, String maskedColName) {
        return df.withColumn(maskedColName,
                functions.when(col(emailColName).isNotNull(),
                        functions.concat(
                                functions.substring(col(emailColName), 1, 3),
                                functions.lit("****"),
                                functions.concat(
                                functions.lit("@"),
                                functions.substring_index(col(emailColName), "@", -1)
                        )
                        )
                ).otherwise(null));
    }

    /**
     * 手机号脱敏（保留前3位和后4位）
     *
     * @param df             待处理的Dataset
     * @param phoneColName   手机号字段名
     * @param maskedColName  脱敏后的字段名
     * @return 添加脱敏字段后的Dataset
     */
    public static Dataset<Row> maskPhone(Dataset<Row> df, String phoneColName, String maskedColName) {
        return df.withColumn(maskedColName,
                functions.when(col(phoneColName).isNotNull(),
                        functions.concat(
                                functions.substring(col(phoneColName), 1, 3),
                                functions.lit("****"),
                                functions.substring(col(phoneColName), -4, 4)
                        )
                ).otherwise(null));
    }

    /**
     * 年龄分组（根据年龄字段生成分组字段）
     *
     * @param df              待处理的Dataset
     * @param ageColName      年龄字段名
     * @param ageGroupColName 分组字段名
     * @return 添加分组字段后的Dataset
     */
    public static Dataset<Row> groupByAge(Dataset<Row> df, String ageColName, String ageGroupColName) {
        return df.withColumn(ageGroupColName,
                functions.when(col(ageColName).isNull(), "未知")
                        .when(col(ageColName).lt(18), "未成年")
                        .when(col(ageColName).between(18, 30), "青年")
                        .when(col(ageColName).between(31, 50), "中年")
                        .otherwise("老年"));
    }

    /**
     * 字段值替换（将指定字段的空值替换为默认值）
     *
     * @param df          待处理的Dataset
     * @param colName     字段名
     * @param defaultValue 默认值
     * @return 替换后的Dataset
     */
    public static Dataset<Row> fillNull(Dataset<Row> df, String colName, Object defaultValue) {
        Map<String, Object> fillMap = new HashMap<>();
        fillMap.put(colName, defaultValue);
        return df.na().fill(fillMap);
    }

    /**
     * 批量字段值替换（将多个字段的空值替换为默认值）
     *
     * @param df           待处理的Dataset
     * @param colNames     字段名数组
     * @param defaultValue 默认值
     * @return 替换后的Dataset
     */
    public static Dataset<Row> fillNull(Dataset<Row> df, String[] colNames, Object defaultValue) {
        Map<String, Object> fillMap = new HashMap<>();
        for (String colName : colNames) {
            fillMap.put(colName, defaultValue);
        }
        return df.na().fill(fillMap);
    }

    /**
     * 去除重复数据-所有字段完全相同
     *
     * @param df 待处理的Dataset
     * @return 去重后的Dataset
     */
    public static Dataset<Row> distinct(Dataset<Row> df) {
        log.info("原始记录数: {}", df.count());
        Dataset<Row> result = df.distinct();
        log.info("所有字段完全相同去重完成，剩余记录数: {}", result.count());
        return result;
    }

    /**
     * 去除重复数据（基于指定字段）
     *
     * @param df        待处理的Dataset
     * @param colNames  用于判断重复的字段名数组
     * @return 去重后的Dataset
     */
    public static Dataset<Row> dropDuplicates(Dataset<Row> df, String[] colNames) {
        log.info("开始基于字段去重，字段数: {}", colNames.length);
        Dataset<Row> result = df.dropDuplicates(colNames);
        log.info("基于字段去重完成，剩余记录数: {}", result.count());
        return result;
    }

    /**
     * 选择输出字段
     *
     * @param df        待处理的Dataset
     * @param colNames  需要输出的字段名数组
     * @return 选择字段后的Dataset
     */
    public static Dataset<Row> selectColumns(Dataset<Row> df, String[] colNames) {
        return df.selectExpr(colNames);
    }

    /**
     * 重命名字段
     *
     * @param df           待处理的Dataset
     * @param oldColName   原字段名
     * @param newColName   新字段名
     * @return 重命名后的Dataset
     */
    public static Dataset<Row> renameColumn(Dataset<Row> df, String oldColName, String newColName) {
        log.info("重命名字段，原字段: {}, 新字段: {}", oldColName, newColName);
        return df.withColumnRenamed(oldColName, newColName);
    }

    /**
     * 批量重命名字段
     *
     * @param df          待处理的Dataset
     * @param colNameMap  字段名映射（旧字段名 -> 新字段名）
     * @return 重命名后的Dataset
     */
    public static Dataset<Row> renameColumns(Dataset<Row> df, Map<String, String> colNameMap) {
        Dataset<Row> result = df;
        for (Map.Entry<String, String> entry : colNameMap.entrySet()) {
            result = result.withColumnRenamed(entry.getKey(), entry.getValue());
        }
        return result;
    }

    // ==================== 分区操作 ====================

    public static Dataset<Row> repartition(Dataset<Row> df, int numPartitions) {
        if (numPartitions <= 0) throw new IllegalArgumentException("分区数必须大于0");
        Dataset<Row> result = df.repartition(numPartitions);
        log.info("DataFrame 重分区完成，分区数: {}", numPartitions);
        return result;
    }

    public static Dataset<Row> repartition(Dataset<Row> df, int numPartitions, String... columnNames) {
        if (numPartitions <= 0) throw new IllegalArgumentException("分区数必须大于0");
        if (columnNames == null || columnNames.length == 0) throw new IllegalArgumentException("分区列不能为空");
        org.apache.spark.sql.Column[] cols = new org.apache.spark.sql.Column[columnNames.length];
        for (int i = 0; i < columnNames.length; i++) cols[i] = org.apache.spark.sql.functions.col(columnNames[i]);
        Dataset<Row> result = df.repartition(numPartitions, cols);
        log.info("DataFrame 按列重分区完成，分区数: {}，列: {}", numPartitions, java.util.Arrays.toString(columnNames));
        return result;
    }

    public static Dataset<Row> coalesce(Dataset<Row> df, int numPartitions) {
        if (numPartitions <= 0) throw new IllegalArgumentException("分区数必须大于0");
        Dataset<Row> result = df.coalesce(numPartitions);
        log.info("DataFrame 合并分区完成，分区数: {}", numPartitions);
        return result;
    }


    public static int getCurrentPartitionNum(Dataset<Row> df) {
        return df.rdd().getNumPartitions();
    }

}
