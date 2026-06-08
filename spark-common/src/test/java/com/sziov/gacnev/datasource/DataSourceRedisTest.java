package com.sziov.gacnev.datasource;

import com.sziov.gacnev.AbstractSparkTest;
import com.sziov.gacnev.common.RedisUtils;
import com.sziov.gacnev.datasource.redis.RedisModel;
import com.sziov.gacnev.datasource.redis.RedisWriteMode;
import io.lettuce.core.api.StatefulRedisConnection;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("Redis 集成测试")
class DataSourceRedisTest extends AbstractSparkTest {

    private final List<String> redisKeys = new ArrayList<>();
    private final List<String> redisStringPrefixes = new ArrayList<>();

    @BeforeAll
    static void initDs() {
        Properties p = new Properties();
        p.setProperty("datasource.redis.host", "localhost");
        p.setProperty("datasource.redis.port", "6379");
        DataSources.init(p);
    }

    @AfterEach
    void cleanup() {
        if (!redisKeys.isEmpty() || !redisStringPrefixes.isEmpty()) {
            StatefulRedisConnection<String, String> conn = RedisUtils.borrowConnection();
            try {
                for (String key : redisKeys) {
                    conn.sync().del(key);
                }
                for (String prefix : redisStringPrefixes) {
                    List<String> keys = RedisUtils.scanAll(prefix + "*", 100);
                    if (!keys.isEmpty()) {
                        conn.sync().del(keys.toArray(new String[0]));
                    }
                }
            } finally {
                RedisUtils.returnConnection(conn);
            }
        }
    }

    private static Dataset<Row> sampleDf() {
        return spark.createDataFrame(Arrays.asList(
                RowFactory.create(1, "Alice"),
                RowFactory.create(2, "Bob")
        ), new StructType(new org.apache.spark.sql.types.StructField[]{
                DataTypes.createStructField("id", DataTypes.IntegerType, false),
                DataTypes.createStructField("name", DataTypes.StringType, true)
        }));
    }

    private static Dataset<Row> largeSampleDf(int count) {
        List<Row> rows = IntStream.range(1, count + 1)
                .mapToObj(i -> RowFactory.create(i, "user_" + i))
                .collect(Collectors.toList());
        return spark.createDataFrame(rows, new StructType(new org.apache.spark.sql.types.StructField[]{
                DataTypes.createStructField("id", DataTypes.IntegerType, false),
                DataTypes.createStructField("name", DataTypes.StringType, true)
        }));
    }

    private static String randomSuffix() {
        return UUID.randomUUID().toString().replace("-", "").substring(0, 8);
    }

    private String uniqueHash() {
        String key = "redis_hash_" + randomSuffix();
        redisKeys.add(key);
        return key;
    }

    private String uniqueStringPrefix() {
        String prefix = "redis_str_" + randomSuffix();
        redisStringPrefixes.add(prefix);
        return prefix;
    }

    private String uniqueSet() {
        String key = "redis_set_" + randomSuffix();
        redisKeys.add(key);
        return key;
    }

    private String uniqueZSet() {
        String key = "redis_zset_" + randomSuffix();
        redisKeys.add(key);
        return key;
    }

    @Test
    @DisplayName("write_read_HASH模型_HSET field json_往返正确")
    void hash_writeRead() {
        String table = uniqueHash();
        DataSources.redis()
                .option(o -> o.setKeyColumn("id").setRedisModel(RedisModel.HASH).setTtl(300))
                .write(sampleDf(), table);
        Dataset<Row> result = DataSources.redis()
                .option(o -> o.setKeyColumn("id").setRedisModel(RedisModel.HASH))
                .read(spark, table);
        result.show();
        assertThat(result.count()).isGreaterThanOrEqualTo(2);
        assertThat(result.schema().fieldNames()).contains("id", "name");
    }

    @Test
    @DisplayName("HASH_SCAN_大数据量hscan分批读取正确")
    void hash_scan_largeHash() {
        String table = uniqueHash();
        int rowCount = 10;
        DataSources.redis()
                .option(o -> o.setKeyColumn("id").setRedisModel(RedisModel.HASH).setTtl(300))
                .write(largeSampleDf(rowCount), table);
        Dataset<Row> result = DataSources.redis()
                .option(o -> o.setKeyColumn("id").setRedisModel(RedisModel.HASH))
                .read(spark, table);
        result.show();
        assertThat(result.count()).isEqualTo(rowCount);
    }

    @Test
    @DisplayName("write_read_STRING模型_SET key json_往返正确")
    void string_writeRead() {
        String keyPrefix = uniqueStringPrefix();
        StructType schema = new StructType(new org.apache.spark.sql.types.StructField[]{
                DataTypes.createStructField("id", DataTypes.StringType, false),
                DataTypes.createStructField("name", DataTypes.StringType, true)
        });
        Dataset<Row> df = spark.createDataFrame(Arrays.asList(
                RowFactory.create(keyPrefix + "_a", "Alice"),
                RowFactory.create(keyPrefix + "_b", "Bob")
        ), schema);

        DataSources.redis()
                .option(o -> o.setKeyColumn("id").setRedisModel(RedisModel.STRING).setTtl(300))
                .write(df, keyPrefix);
        Dataset<Row> result = DataSources.redis()
                .option(o -> o.setKeyColumn("id").setRedisModel(RedisModel.STRING)
                        .setKeysPattern(keyPrefix + "_*").setScanCount(10))
                .read(spark, keyPrefix);
        result.show();
        assertThat(result.count()).isEqualTo(2);
        assertThat(result.schema().fieldNames()).contains("_key", "value");
    }

    @Test
    @DisplayName("SCAN_keysPattern_通配匹配多个key")
    void scan_keysPattern_multiKey() {
        String prefix = uniqueStringPrefix();
        StructType schema = new StructType(new org.apache.spark.sql.types.StructField[]{
                DataTypes.createStructField("id", DataTypes.StringType, false),
                DataTypes.createStructField("name", DataTypes.StringType, true)
        });
        Dataset<Row> df = spark.createDataFrame(Arrays.asList(
                RowFactory.create(prefix + "_a", "A"),
                RowFactory.create(prefix + "_b", "B"),
                RowFactory.create(prefix + "_c", "C")
        ), schema);

        DataSources.redis()
                .option(o -> o.setKeyColumn("id").setRedisModel(RedisModel.STRING).setTtl(300))
                .write(df, prefix);

        Dataset<Row> result = DataSources.redis()
                .option(o -> o.setKeyColumn("id").setRedisModel(RedisModel.STRING)
                        .setKeysPattern(prefix + "_*").setScanCount(5))
                .read(spark, prefix);
        result.show();
        assertThat(result.count()).isEqualTo(3);
    }

    @Test
    @DisplayName("SCAN_scanCount_小批量扫描多key")
    void scan_smallScanCount() {
        String prefix = uniqueStringPrefix();
        StructType schema = new StructType(new org.apache.spark.sql.types.StructField[]{
                DataTypes.createStructField("id", DataTypes.StringType, false),
                DataTypes.createStructField("name", DataTypes.StringType, true)
        });
        List<Row> rows = IntStream.range(0, 5)
                .mapToObj(i -> RowFactory.create(prefix + "_" + i, "val_" + i))
                .collect(Collectors.toList());
        Dataset<Row> df = spark.createDataFrame(rows, schema);

        DataSources.redis()
                .option(o -> o.setKeyColumn("id").setRedisModel(RedisModel.STRING).setTtl(300))
                .write(df, prefix);

        Dataset<Row> result = DataSources.redis()
                .option(o -> o.setKeyColumn("id").setRedisModel(RedisModel.STRING)
                        .setKeysPattern(prefix + "_*").setScanCount(2))
                .read(spark, prefix);
        result.show();
        assertThat(result.count()).isEqualTo(5);
    }

    @Test
    @DisplayName("write_read_SET模型_SADD set json_往返正确")
    void set_writeRead() {
        String setKey = uniqueSet();
        DataSources.redis()
                .option(o -> o.setKeyColumn("id").setRedisModel(RedisModel.SET).setTtl(300))
                .write(sampleDf(), setKey);
        Dataset<Row> result = DataSources.redis()
                .option(o -> o.setKeyColumn("id").setRedisModel(RedisModel.SET))
                .read(spark, setKey);
        result.show();
        assertThat(result.count()).isEqualTo(1);
        String value = result.collectAsList().get(0).getString(1);
        assertThat(value).contains("Alice").contains("Bob");
    }

    @Test
    @DisplayName("write_read_ZSET模型_ZADD zset score json_往返正确")
    void zset_writeRead() {
        String zsetKey = uniqueZSet();
        DataSources.redis()
                .option(o -> o.setKeyColumn("id").setRedisModel(RedisModel.ZSET)
                        .setZsetScoreColumn("id").setTtl(300))
                .write(sampleDf(), zsetKey);
        Dataset<Row> result = DataSources.redis()
                .option(o -> o.setKeyColumn("id").setRedisModel(RedisModel.ZSET))
                .read(spark, zsetKey);
        result.show();
        assertThat(result.count()).isEqualTo(1);
        String value = result.collectAsList().get(0).getString(1);
        assertThat(value).contains("Alice").contains("Bob");
    }

    // ==================== 写入模式测试 ====================

    @Test
    @DisplayName("write_Direct模式_同步逐条写入_往返正确")
    void write_direct() {
        String table = uniqueHash();
        DataSources.redis()
                .option(o -> o.setKeyColumn("id").setRedisModel(RedisModel.HASH)
                        .setRedisWriteMode(RedisWriteMode.DIRECT).setTtl(300))
                .write(sampleDf(), table);
        Dataset<Row> result = DataSources.redis()
                .option(o -> o.setKeyColumn("id").setRedisModel(RedisModel.HASH))
                .read(spark, table);
        assertThat(result.count()).isEqualTo(2);
    }

    @Test
    @DisplayName("write_Lua模式_EVAL原子写入_往返正确")
    void write_lua() {
        String table = uniqueHash();
        DataSources.redis()
                .option(o -> o.setKeyColumn("id").setRedisModel(RedisModel.HASH)
                        .setRedisWriteMode(RedisWriteMode.LUA).setTtl(300))
                .write(sampleDf(), table);
        Dataset<Row> result = DataSources.redis()
                .option(o -> o.setKeyColumn("id").setRedisModel(RedisModel.HASH))
                .read(spark, table);
        assertThat(result.count()).isEqualTo(2);
    }

    @Test
    @DisplayName("write_Pipeline模式_批量写入_往返正确")
    void write_pipeline() {
        String table = uniqueHash();
        DataSources.redis()
                .option(o -> o.setKeyColumn("id").setRedisModel(RedisModel.HASH)
                        .setRedisWriteMode(RedisWriteMode.PIPELINE).setTtl(300))
                .write(sampleDf(), table);
        Dataset<Row> result = DataSources.redis()
                .option(o -> o.setKeyColumn("id").setRedisModel(RedisModel.HASH))
                .read(spark, table);
        assertThat(result.count()).isEqualTo(2);
    }

    @Test
    @DisplayName("write_Transaction模式_MULTI_EXEC原子写入_往返正确")
    void write_transaction() {
        String table = uniqueHash();
        DataSources.redis()
                .option(o -> o.setKeyColumn("id").setRedisModel(RedisModel.HASH)
                        .setRedisWriteMode(RedisWriteMode.TRANSACTION).setTtl(300))
                .write(sampleDf(), table);
        Dataset<Row> result = DataSources.redis()
                .option(o -> o.setKeyColumn("id").setRedisModel(RedisModel.HASH))
                .read(spark, table);
        assertThat(result.count()).isEqualTo(2);
    }

    @Test
    @DisplayName("write_AsyncCallback模式_异步回调写入_往返正确")
    void write_asyncCallback() {
        String table = uniqueHash();
        DataSources.redis()
                .option(o -> o.setKeyColumn("id").setRedisModel(RedisModel.HASH)
                        .setRedisWriteMode(RedisWriteMode.ASYNC_CALLBACK).setTtl(300))
                .write(sampleDf(), table);
        Dataset<Row> result = DataSources.redis()
                .option(o -> o.setKeyColumn("id").setRedisModel(RedisModel.HASH))
                .read(spark, table);
        assertThat(result.count()).isEqualTo(2);
    }

    @Test
    @DisplayName("write_Lua_自定义脚本_幂等写入_仅变化时更新")
    void write_lua_idempotentUpsert() {
        String table = uniqueHash();
        // 场景：每日 ETL 重跑时避免重复写相同数据
        // Lua: HGET 已有值 → 仅当新值与旧值不同时才 HSET
        String script =
                "local existing = redis.call('HGET', KEYS[1], ARGV[1]) " +
                "if not existing or existing ~= ARGV[2] then " +
                "  redis.call('HSET', KEYS[1], ARGV[1], ARGV[2]) " +
                "  if tonumber(ARGV[3]) > 0 then redis.call('EXPIRE', KEYS[1], ARGV[3]) end " +
                "  return 1 " +
                "end " +
                "return 0";

        // Step 1: 首次写入 {id=1, "Alice"}, {id=2, "Bob"}
        Dataset<Row> batch1 = spark.createDataFrame(Arrays.asList(
                RowFactory.create(1, "Alice"),
                RowFactory.create(2, "Bob")
        ), new StructType(new org.apache.spark.sql.types.StructField[]{
                DataTypes.createStructField("id", DataTypes.IntegerType, false),
                DataTypes.createStructField("name", DataTypes.StringType, true)
        }));
        DataSources.redis()
                .option(o -> o.setKeyColumn("id").setRedisModel(RedisModel.HASH)
                        .setRedisWriteMode(RedisWriteMode.LUA)
                        .setLuaScript(script).setTtl(300))
                .write(batch1, table);

        Dataset<Row> r1 = DataSources.redis()
                .option(o -> o.setKeyColumn("id").setRedisModel(RedisModel.HASH))
                .read(spark, table);
        r1.show();
        assertThat(r1.count()).isEqualTo(2);
        assertThat(r1.filter("id=1").select("name").first().getString(0)).isEqualTo("Alice");

        // Step 2: 只改 id=1 → "Alice_v2"，id=2 不变
        Dataset<Row> batch2 = spark.createDataFrame(Arrays.asList(
                RowFactory.create(1, "Alice_v2"),
                RowFactory.create(2, "Bob")
        ), new StructType(new org.apache.spark.sql.types.StructField[]{
                DataTypes.createStructField("id", DataTypes.IntegerType, false),
                DataTypes.createStructField("name", DataTypes.StringType, true)
        }));
        DataSources.redis()
                .option(o -> o.setKeyColumn("id").setRedisModel(RedisModel.HASH)
                        .setRedisWriteMode(RedisWriteMode.LUA)
                        .setLuaScript(script).setTtl(300))
                .write(batch2, table);

        Dataset<Row> r2 = DataSources.redis()
                .option(o -> o.setKeyColumn("id").setRedisModel(RedisModel.HASH))
                .read(spark, table);
        r2.show();
        assertThat(r2.count()).isEqualTo(2);
        assertThat(r2.filter("id=1").select("name").first().getString(0)).isEqualTo("Alice_v2");
        assertThat(r2.filter("id=2").select("name").first().getString(0)).isEqualTo("Bob");

        // Step 3: 再次写相同数据，应全部跳过
        DataSources.redis()
                .option(o -> o.setKeyColumn("id").setRedisModel(RedisModel.HASH)
                        .setRedisWriteMode(RedisWriteMode.LUA)
                        .setLuaScript(script).setTtl(300))
                .write(batch2, table);

        Dataset<Row> r3 = DataSources.redis()
                .option(o -> o.setKeyColumn("id").setRedisModel(RedisModel.HASH))
                .read(spark, table);
        r3.show();
        assertThat(r3.count()).isEqualTo(2);
        assertThat(r3.filter("id=1").select("name").first().getString(0)).isEqualTo("Alice_v2");
        assertThat(r3.filter("id=2").select("name").first().getString(0)).isEqualTo("Bob");
    }


}
