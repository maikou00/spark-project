package com.sziov.gacnev.orderstats.pojo;

import lombok.Data;
import java.io.Serializable;
import java.math.BigDecimal;

@Data
public class DwsOrderDaily implements Serializable {
    private static final long serialVersionUID = 1L;
    private String dimType;
    private String dimId;
    private Long orderCount;
    private BigDecimal totalAmount;
    private Long paidCount;
    private Long refundCount;
    private String dt;
}
