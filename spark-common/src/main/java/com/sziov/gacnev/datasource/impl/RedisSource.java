package com.sziov.gacnev.datasource.impl;

import com.sziov.gacnev.common.JsonUtils;
import com.sziov.gacnev.common.RedisUtils;
import com.sziov.gacnev.common.WarehouseException;
import com.sziov.gacnev.datasource.DataSink;
import com.sziov.gacnev.datasource.DataSource;
import com.sziov.gacnev.datasource.DataSourceProvider;
import com.sziov.gacnev.datasource.DataSourceType;
import com.sziov.gacnev.datasource.option.RedisOption;
import com.sziov.gacnev.datasource.redis.RedisModel;
import com.sziov.gacnev.datasource.redis.RedisReads;
import io.lettuce.core.api.StatefulRedisConnection;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Redis 数据读取，通过 Lettuce mapPartitions 实现。
 * hash 模型走 HSCAN 分批读取，其他模型支持 keysPattern 通配 SCAN 匹配。
 *
 * @author maikou
 * @since 2026-06-10
 */
@Slf4j
public class RedisSource implements DataSource<RedisOption>, DataSourceProvider, Serializable {

    private static final long serialVersionUID = 1L;

    private static final int DEFAULT_SCAN_COUNT = 100;
    private static final int DEFAULT_PARTITIONS = 4;
        private static final String KEY_COLUMN = "_key";

    @Override
    public DataSourceType type() {
        return DataSourceType.REDIS;
    }

    @Override
    public DataSource<?> createSource() {
        return this;
    }

    @Override
    public DataSink<?> createSink() {
        return new RedisSink();
    }

    @Override
    public Dataset<Row> read(SparkSession spark, RedisOption options) {
        
    RedisModel model = options.getRedisModel() != null
            ? options.getRedisModel() : RedisModel.HASH;
    String resource = options.getResource();
    if (resource == null || resource.isEmpty()) {
        throw new WarehouseException("Redis 读取必须指定 resource");
    }

    if (model.isHash()) {
        return readHash(spark, resource, options);
    }
    return readNonHash(spark, options, model);
        
    }

    /** hash 模型：HSCAN + HMGET 分批读取，避免大表阻塞 Redis */
    private Dataset<Row> readHash(SparkSession spark, String hashKey, RedisOption options) {
        String keyColumn = options.getKeyColumn() != null && !options.getKeyColumn().isEmpty()
                ? options.getKeyColumn() : KEY_COLUMN;

        List<Row> allRows = new ArrayList<>();
        StructType schema = null;
        StatefulRedisConnection<String, String> conn = RedisUtils.borrowConnection();
        try {
            io.lettuce.core.ScanCursor cursor = io.lettuce.core.ScanCursor.INITIAL;
            io.lettuce.core.ScanArgs scanArgs = io.lettuce.core.ScanArgs.Builder.limit(200);
            do {
                io.lettuce.core.MapScanCursor<String, String> result = conn.sync().hscan(hashKey, cursor, scanArgs);
                Map<String, String> batch = result.getMap();
                if (batch != null && !batch.isEmpty()) {
                    List<String> fields = new ArrayList<>(batch.keySet());
                    List<io.lettuce.core.KeyValue<String, String>> values = conn.sync().hmget(hashKey, fields.toArray(new String[0]));
                    if (schema == null) {
                        schema = inferHashSchemaFromKv(values, keyColumn);
                        if (schema == null) {
                            log.warn("RedisSource 无法推断 hash Schema，key: {}", hashKey);
                            return spark.emptyDataFrame();
                        }
                    }
                    parseHashBatch(allRows, values, schema, keyColumn);
                }
                cursor = result;
            } while (!cursor.isFinished());

            if (allRows.isEmpty()) {
                log.warn("RedisSource hash 读取结果为空，key: {}", hashKey);
                return spark.emptyDataFrame();
            }
            log.info("RedisSource hash 读取完成，rows: {}", allRows.size());
            return spark.createDataFrame(allRows, schema);
        } catch (Exception e) {
            log.error("RedisSource hash 读取失败", e);
            throw new WarehouseException("Redis hash 读取失败", e);
        } finally {
            RedisUtils.returnConnection(conn);
        }
    }

    private StructType inferHashSchemaFromKv(List<io.lettuce.core.KeyValue<String, String>> kvs, String keyColumn) {
        for (io.lettuce.core.KeyValue<String, String> kv : kvs) {
            if (!kv.hasValue()) continue;
            Map<String, Object> rowMap = JsonUtils.fromJsonToMap(kv.getValue());
            if (rowMap != null && !rowMap.isEmpty()) {
                List<StructField> fields = new ArrayList<>();
                fields.add(DataTypes.createStructField(keyColumn, DataTypes.StringType, false));
                for (String colName : rowMap.keySet()) {
                    if (!colName.equals(keyColumn)) {
                        fields.add(DataTypes.createStructField(colName, DataTypes.StringType, true));
                    }
                }
                log.info("RedisSource hash Schema 推断完成，{} 个字段", fields.size());
                return DataTypes.createStructType(fields);
            }
        }
        return null;
    }

    private void parseHashBatch(List<Row> rows, List<io.lettuce.core.KeyValue<String, String>> kvs,
                                 StructType schema, String keyColumn) {
        String[] fieldNames = schema.fieldNames();
        for (io.lettuce.core.KeyValue<String, String> kv : kvs) {
            if (!kv.hasValue()) continue;
            String field = kv.getKey();
            Map<String, Object> rowMap = JsonUtils.fromJsonToMap(kv.getValue());
            if (rowMap == null) continue;
            Object[] values = new Object[fieldNames.length];
            for (int i = 0; i < fieldNames.length; i++) {
                if (keyColumn.equals(fieldNames[i])) {
                    values[i] = field;
                } else {
                    Object val = rowMap.get(fieldNames[i]);
                    values[i] = val != null ? val.toString() : null;
                }
            }
            rows.add(RowFactory.create(values));
        }
    }

    /** 非 hash 模型：SCAN + mapPartitions + 策略 */
    private Dataset<Row> readNonHash(SparkSession spark, RedisOption options, RedisModel model) {
        String keyColumn = options.getKeyColumn() != null && !options.getKeyColumn().isEmpty()
                ? options.getKeyColumn() : KEY_COLUMN;
        String keysPattern = options.getKeysPattern();
        String resource = options.getResource();

        List<String> keys;
        if (keysPattern != null && !keysPattern.isEmpty()) {
            int scanCount = options.getScanCount() > 0 ? options.getScanCount() : DEFAULT_SCAN_COUNT;
            keys = RedisUtils.scanAll(keysPattern, scanCount);
        } else {
            keys = Collections.singletonList(resource);
        }
        if (keys.isEmpty()) {
            log.warn("RedisSource 未匹配到任何 key");
            return spark.emptyDataFrame();
        }
        log.info("RedisSource SCAN 匹配到 {} 个 key", keys.size());

        int numPartitions = options.getNumPartitions() > 0 ? options.getNumPartitions() : DEFAULT_PARTITIONS;
        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField(KEY_COLUMN, DataTypes.StringType, false),
                DataTypes.createStructField("value", DataTypes.StringType, true)
        });

        Dataset<String> keyDs = spark.createDataset(keys, Encoders.STRING()).repartition(numPartitions);
        JavaRDD<Row> rowRdd = keyDs.toJavaRDD().mapPartitions(
                keyIter -> readPartition(keyIter, keyColumn, schema, model));
        return spark.createDataFrame(rowRdd, schema);
    }

    private Iterator<Row> readPartition(Iterator<String> keyIter, String keyColumn,
                                         StructType schema, RedisModel model) {
        List<Row> rows = new ArrayList<>();
        StatefulRedisConnection<String, String> conn = RedisUtils.borrowConnection();
        try {
            while (keyIter.hasNext()) {
                String key = keyIter.next();
                Row row = RedisReads.readRow(model, conn, key, schema);
                if (row != null) {
                    rows.add(row);
                }
            }
        } catch (Exception e) {
            log.error("Redis 分区读取失败", e);
            throw new WarehouseException("Redis 分区读取失败", e);
        } finally {
            RedisUtils.returnConnection(conn);
        }
        return rows.iterator();
    }
}
