package com.sziov.gacnev.orderstats.pojo;

import lombok.Data;
import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Date;

@Data
public class AdsOrderKpi implements Serializable {
    private static final long serialVersionUID = 1L;
    private Date dt;
    private Long totalOrders;
    private BigDecimal totalGmv;
    private BigDecimal avgOrderAmount;
    private Long paidOrders;
    private BigDecimal paymentRate;
    private Long refundOrders;
    private BigDecimal refundRate;
}
