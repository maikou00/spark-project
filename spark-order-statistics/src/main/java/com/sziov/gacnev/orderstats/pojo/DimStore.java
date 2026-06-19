package com.sziov.gacnev.orderstats.pojo;

import lombok.Data;
import java.io.Serializable;
import java.math.BigDecimal;

@Data
public class DimStore implements Serializable {
    private static final long serialVersionUID = 1L;
    private String storeId;
    private String storeName;
    private String storeType;
    private BigDecimal rating;
}
