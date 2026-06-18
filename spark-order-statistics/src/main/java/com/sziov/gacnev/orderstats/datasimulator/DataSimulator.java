package com.sziov.gacnev.orderstats.datasimulator;

import com.sziov.gacnev.utils.JsonUtils;
import com.sziov.gacnev.datasource.DataSources;
import org.apache.spark.sql.SaveMode;
import static org.apache.spark.sql.functions.lit;
import com.sziov.gacnev.orderstats.config.OrderStatsConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static com.sziov.gacnev.orderstats.config.OrderStatsConfig.*;

/**
 * 数据模拟器：生成订单事件和维度数据的模拟数据，并写入 Hive ODS 层。
 *
 * @author maikou
 * @since 2026-06-09
 */
@Slf4j
public final class DataSimulator {

    private static final Random RANDOM = new Random();
    private static final DateTimeFormatter DATE_FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter TIME_FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private static final String[] FIRST_NAMES = {"张", "李", "王", "刘", "陈", "杨", "黄", "赵", "周", "吴",
            "徐", "孙", "马", "朱", "胡", "郭", "何", "高", "林", "罗"};
    private static final String[] LAST_NAMES = {"伟", "芳", "娜", "敏", "静", "丽", "强", "磊", "军", "洋",
            "勇", "艳", "杰", "娟", "涛", "明", "超", "秀英", "华", "慧"};
    private static final String[] CATEGORIES = {"电子产品", "服装鞋帽", "食品饮料", "家居用品",
            "图书音像", "运动户外", "美妆护肤", "母婴用品"};
    private static final String[] STORE_NAMES = {"旗舰数码店", "时尚服饰馆", "美味食品坊", "温馨家居城",
            "知识书屋", "动力运动营", "美丽密码", "宝贝乐园", "官方自营旗舰店", "优品生活馆"};
    private static final String[][] REGIONS = {
            {"110000", "北京市", null, "province"},
            {"310000", "上海市", null, "province"},
            {"440000", "广东省", null, "province"},
            {"440300", "深圳市", "440000", "city"},
            {"440100", "广州市", "440000", "city"},
            {"330000", "浙江省", null, "province"},
            {"330100", "杭州市", "330000", "city"},
            {"500000", "重庆市", null, "province"},
            {"510000", "四川省", null, "province"},
            {"510100", "成都市", "510000", "city"},
            {"320000", "江苏省", null, "province"},
            {"320100", "南京市", "320000", "city"},
            {"120000", "天津市", null, "province"},
            {"420000", "湖北省", null, "province"},
            {"420100", "武汉市", "420000", "city"}
    };

    private final SparkSession spark;
    private final String dt;
    private final List<Map<String, String>> users = new ArrayList<>();
    private final List<Map<String, String>> products = new ArrayList<>();
    private final List<Map<String, String>> stores = new ArrayList<>();
    private int dirtyEmptyIdCount;
    private int dirtyBadJsonCount;
    private int dirtyDuplicateCount;

    private DataSimulator(SparkSession spark, String dt) {
        this.spark = spark;
        this.dt = dt;
    }

    public static void generate(SparkSession spark, String dt) {
        DataSimulator simulator = new DataSimulator(spark, dt);
        simulator.generateDimensionData();
        simulator.generateOrderEvents();
    }

    private void generateDimensionData() {
        generateUsers();
        generateProducts();
        generateStores();
    }

    private void generateUsers() {
        List<Row> rows = new ArrayList<>();
        for (int i = 0; i < OrderStatsConfig.SIM_USER_COUNT; i++) {
            Map<String, String> user = new HashMap<>();
            String userId = "U" + String.format("%04d", i + 1);
            user.put("user_id", userId);
            user.put("user_name", FIRST_NAMES[RANDOM.nextInt(FIRST_NAMES.length)]
                    + LAST_NAMES[RANDOM.nextInt(LAST_NAMES.length)]);
            user.put("phone", "138" + String.format("%08d", RANDOM.nextInt(100000000)));
            user.put("email", userId.toLowerCase() + "@example.com");
            String registerDate = LocalDate.parse(dt, DATE_FMT)
                    .minusDays(RANDOM.nextInt(365))
                    .format(DATE_FMT);
            String regionId = REGIONS[RANDOM.nextInt(REGIONS.length)][0];

            user.put("register_date", registerDate);
            user.put("region_id", regionId);
            users.add(user);

            rows.add(RowFactory.create(userId, user.get("user_name"),
                    user.get("phone"), user.get("email"), registerDate, regionId));
        }

        StructType schema = new StructType()
                .add("user_id", DataTypes.StringType)
                .add("user_name", DataTypes.StringType)
                .add("phone", DataTypes.StringType)
                .add("email", DataTypes.StringType)
                .add("register_date", DataTypes.StringType)
                .add("region_id", DataTypes.StringType);

        Dataset<Row> df = spark.createDataFrame(rows, schema);
        writeToOds("ods_user", df);
    }

    private void generateProducts() {
        List<Row> rows = new ArrayList<>();
        for (int i = 0; i < OrderStatsConfig.SIM_PRODUCT_COUNT; i++) {
            Map<String, String> product = new HashMap<>();
            String productId = "P" + String.format("%04d", i + 1);
            String category = CATEGORIES[RANDOM.nextInt(CATEGORIES.length)];
            String productName = category.replaceAll("[^\\u4e00-\\u9fa5]", "")
                    + RANDOM.nextInt(100);

            product.put("product_id", productId);
            product.put("product_name", productName);
            product.put("category", category);
            products.add(product);

            BigDecimal unitPrice = BigDecimal.valueOf(10 + RANDOM.nextDouble() * 990)
                    .setScale(2, RoundingMode.HALF_UP);
            int stock = 50 + RANDOM.nextInt(450);

            rows.add(RowFactory.create(productId, productName, category, unitPrice, stock));
        }

        StructType schema = new StructType()
                .add("product_id", DataTypes.StringType)
                .add("product_name", DataTypes.StringType)
                .add("category", DataTypes.StringType)
                .add("unit_price", DataTypes.createDecimalType(18, 2))
                .add("stock", DataTypes.IntegerType);

        Dataset<Row> df = spark.createDataFrame(rows, schema);
        writeToOds("ods_product", df);
    }

    private void generateStores() {
        List<Row> rows = new ArrayList<>();
        for (int i = 0; i < OrderStatsConfig.SIM_STORE_COUNT; i++) {
            Map<String, String> store = new HashMap<>();
            String storeId = "S" + String.format("%04d", i + 1);
            store.put("store_id", storeId);
            store.put("store_name", STORE_NAMES[i]);
            stores.add(store);

            String storeType = i == OrderStatsConfig.SIM_SELF_OPERATED_STORE_INDEX ? "self" : "third";
            BigDecimal rating = BigDecimal.valueOf(3.0 + RANDOM.nextDouble() * 2.0)
                    .setScale(1, RoundingMode.HALF_UP);

            rows.add(RowFactory.create(storeId, STORE_NAMES[i], storeType, rating));
        }

        StructType schema = new StructType()
                .add("store_id", DataTypes.StringType)
                .add("store_name", DataTypes.StringType)
                .add("store_type", DataTypes.StringType)
                .add("rating", DataTypes.createDecimalType(3, 1));

        Dataset<Row> df = spark.createDataFrame(rows, schema);
        writeToOds("ods_store", df);
    }

    private void generateRegions() {
        List<Row> rows = new ArrayList<>();
        for (String[] r : REGIONS) {
            rows.add(RowFactory.create((Object[]) r));
        }

        StructType schema = new StructType()
                .add("region_id", DataTypes.StringType)
                .add("region_name", DataTypes.StringType)
                .add("parent_region_id", DataTypes.StringType)
                .add("region_level", DataTypes.StringType);

        Dataset<Row> df = spark.createDataFrame(rows, schema);
        writeToOds("ods_region", df);
    }

    private void writeToOds(String tableName, Dataset<Row> df) {
        spark.sql("ALTER TABLE ods." + tableName
                + " DROP IF EXISTS PARTITION (dt='" + dt + "')");
        DataSources.hive()
                .option(o -> o.setDatabase(OrderStatsConfig.DB_ODS)
                        .setWriteMode(SaveMode.Append))
                .write(df.withColumn(OrderStatsConfig.PART_DT, lit(dt)), tableName);
    }

    private void generateOrderEvents() {
        List<Row> rows = new ArrayList<>();
        int expectedDirtyEmptyId = (int) (OrderStatsConfig.SIM_ORDER_COUNT * OrderStatsConfig.SIM_DIRTY_EMPTY_ID_RATIO);
        int expectedDirtyBadJson = (int) (OrderStatsConfig.SIM_ORDER_COUNT * OrderStatsConfig.SIM_DIRTY_BAD_JSON_RATIO);
        int expectedDirtyDuplicate = (int) (OrderStatsConfig.SIM_ORDER_COUNT * OrderStatsConfig.SIM_DIRTY_DUPLICATE_RATIO);

        int totalEvents = 0;
        for (int i = 0; i < OrderStatsConfig.SIM_ORDER_COUNT; i++) {
            String orderId = "ORD" + String.format("%06d", i + 1);
            Map<String, String> user = users.get(RANDOM.nextInt(users.size()));
            Map<String, String> product = products.get(RANDOM.nextInt(products.size()));
            Map<String, String> store = stores.get(RANDOM.nextInt(stores.size()));
            String regionId = REGIONS[RANDOM.nextInt(REGIONS.length)][0];

            List<String> eventTypes = generateEventSequence();
            LocalDate baseDate = LocalDate.parse(dt, DATE_FMT);

            for (int j = 0; j < eventTypes.size(); j++) {
                String eventType = eventTypes.get(j);
                String eventId = "EVT" + String.format("%010d", totalEvents + 1);

                Map<String, Object> orderData = new HashMap<>();
                orderData.put("order_id", orderId);
                orderData.put("user_id", user.get("user_id"));
                orderData.put("product_id", product.get("product_id"));
                orderData.put("store_id", store.get("store_id"));
                orderData.put("region_id", regionId);
                orderData.put("order_amount", String.format("%.2f",
                        10 + RANDOM.nextDouble() * 990));
                orderData.put("order_status", eventType);

                String eventTime = baseDate.atTime(9 + j, RANDOM.nextInt(60), RANDOM.nextInt(60)).format(TIME_FMT);
                orderData.put("create_time", j == 0 ? eventTime : "");
                orderData.put("pay_time", eventType.equals(EVENT_PAY) || j > 1 ? eventTime : "");
                orderData.put("ship_time", eventType.equals(EVENT_SHIP) || j > 2 ? eventTime : "");
                orderData.put("sign_time", eventType.equals(EVENT_SIGN) || j > 3 ? eventTime : "");
                orderData.put("refund_time", eventType.equals(EVENT_REFUND) ? eventTime : "");

                String eventData = JsonUtils.toJson(orderData);

                if (dirtyEmptyIdCount < expectedDirtyEmptyId && RANDOM.nextDouble() < 0.5) {
                    eventId = "";
                    dirtyEmptyIdCount++;
                }
                if (dirtyBadJsonCount < expectedDirtyBadJson && RANDOM.nextDouble() < 0.5) {
                    eventData = "DEFINITELY_NOT_JSON";
                    dirtyBadJsonCount++;
                }

                rows.add(RowFactory.create(eventId, eventType, eventData, eventTime));
                totalEvents++;
            }
        }

        for (int i = 0; i < expectedDirtyDuplicate && !rows.isEmpty(); i++) {
            Row src = rows.get(RANDOM.nextInt(rows.size()));
            rows.add(RowFactory.create(src.getString(0), src.getString(1),
                    src.getString(2), src.getString(3)));
            dirtyDuplicateCount++;
            totalEvents++;
        }

        Collections.shuffle(rows, RANDOM);

        StructType schema = new StructType()
                .add("event_id", DataTypes.StringType)
                .add("event_type", DataTypes.StringType)
                .add("event_data", DataTypes.StringType)
                .add("event_time", DataTypes.StringType);

        Dataset<Row> df = spark.createDataFrame(rows, schema);
        writeToOds("ods_order_event", df);
    }

    private List<String> generateEventSequence() {
        List<String> events = new ArrayList<>(Collections.singletonList(EVENT_CREATE));
        if (RANDOM.nextDouble() < OrderStatsConfig.SIM_PAY_RATE) {
            events.add(EVENT_PAY);
            if (RANDOM.nextDouble() < OrderStatsConfig.SIM_SHIP_RATE) {
                events.add(EVENT_SHIP);
                if (RANDOM.nextDouble() < OrderStatsConfig.SIM_SIGN_RATE) {
                    events.add(EVENT_SIGN);
                }
            }
        }
        if (events.contains(EVENT_SIGN) && RANDOM.nextDouble() < OrderStatsConfig.SIM_REFUND_RATE) {
            events.add(EVENT_REFUND);
        }
        return events;
    }
}
