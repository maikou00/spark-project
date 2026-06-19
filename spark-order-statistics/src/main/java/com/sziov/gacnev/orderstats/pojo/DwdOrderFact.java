package com.sziov.gacnev.orderstats.pojo;

import lombok.Data;
import java.io.Serializable;
import java.math.BigDecimal;

@Data
public class DwdOrderFact implements Serializable {
    private static final long serialVersionUID = 1L;
    private String order_id;
    private String user_id;
    private String product_id;
    private String store_id;
    private String region_id;
    private BigDecimal order_amount;
    private String order_status;
    private String create_time;
    private String pay_time;
    private String ship_time;
    private String sign_time;
    private String refund_time;
    private String dt;
}
