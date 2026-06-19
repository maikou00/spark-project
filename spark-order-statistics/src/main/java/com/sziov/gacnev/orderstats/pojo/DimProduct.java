package com.sziov.gacnev.orderstats.pojo;

import lombok.Data;
import java.io.Serializable;
import java.math.BigDecimal;

@Data
public class DimProduct implements Serializable {
    private static final long serialVersionUID = 1L;
    private String productId;
    private String productName;
    private String category;
    private BigDecimal unitPrice;
}
