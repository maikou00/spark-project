package com.sziov.gacnev.orderstats.schema;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

/**
 * 订单统计模块 Schema 定义。
 *
 * @author maikou
 * @since 2026-06-09
 */
public final class OrderStatsSchema {

    private OrderStatsSchema() {
        throw new UnsupportedOperationException("常量类不允许实例化");
    }

    public static final StructType ORDER_EVENT = new StructType()
            .add("order_id", DataTypes.StringType, false)
            .add("user_id", DataTypes.StringType, false)
            .add("product_id", DataTypes.StringType, false)
            .add("store_id", DataTypes.StringType, false)
            .add("region_id", DataTypes.StringType, false)
            .add("order_amount", DataTypes.createDecimalType(18, 2), false)
            .add("order_status", DataTypes.StringType, false)
            .add("create_time", DataTypes.StringType, true)
            .add("pay_time", DataTypes.StringType, true)
            .add("ship_time", DataTypes.StringType, true)
            .add("sign_time", DataTypes.StringType, true)
            .add("refund_time", DataTypes.StringType, true);

    public static final StructType DWS_ORDER_DAILY = new StructType()
            .add("dim_type", DataTypes.StringType, false)
            .add("dim_id", DataTypes.StringType, false)
            .add("order_count", DataTypes.LongType, false)
            .add("total_amount", DataTypes.createDecimalType(18, 2), false)
            .add("paid_count", DataTypes.LongType, false)
            .add("refund_count", DataTypes.LongType, false);

    public static final StructType ADS_ORDER_KPI = new StructType()
            .add("total_orders", DataTypes.LongType, false)
            .add("total_gmv", DataTypes.createDecimalType(18, 2), false)
            .add("avg_order_amount", DataTypes.createDecimalType(18, 2), false)
            .add("paid_orders", DataTypes.LongType, false)
            .add("payment_rate", DataTypes.createDecimalType(5, 4), false)
            .add("refund_orders", DataTypes.LongType, false)
            .add("refund_rate", DataTypes.createDecimalType(5, 4), false);
}
