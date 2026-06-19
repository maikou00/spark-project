-- ============================================================================
-- Order Statistics - MySQL 维度表初始化脚本
-- ============================================================================

CREATE TABLE IF NOT EXISTS dim_user (
    user_id       VARCHAR(16)    NOT NULL PRIMARY KEY COMMENT '用户ID',
    user_name     VARCHAR(32)    COMMENT '用户名',
    phone         VARCHAR(16)    COMMENT '手机号',
    email         VARCHAR(64)    COMMENT '邮箱',
    region_id     VARCHAR(16)    COMMENT '地区ID',
    register_date VARCHAR(16)    COMMENT '注册日期'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS dim_product (
    product_id   VARCHAR(16)    NOT NULL PRIMARY KEY COMMENT '商品ID',
    product_name VARCHAR(64)    COMMENT '商品名称',
    category     VARCHAR(32)    COMMENT '类目',
    unit_price   DECIMAL(18,2)  COMMENT '单价'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS dim_store (
    store_id   VARCHAR(16)    NOT NULL PRIMARY KEY COMMENT '店铺ID',
    store_name VARCHAR(32)    COMMENT '店铺名称',
    store_type VARCHAR(8)     COMMENT '店铺类型: self/third',
    rating     DECIMAL(3,1)   COMMENT '评级'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS dim_region (
    region_id       VARCHAR(16) NOT NULL PRIMARY KEY COMMENT '地区ID',
    region_name     VARCHAR(32) COMMENT '地区名称',
    parent_region_id VARCHAR(16) COMMENT '父地区ID',
    region_level    VARCHAR(16) COMMENT '地区级别: province/city/district'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
