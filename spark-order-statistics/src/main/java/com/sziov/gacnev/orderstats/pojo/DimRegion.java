package com.sziov.gacnev.orderstats.pojo;

import lombok.Data;
import java.io.Serializable;

@Data
public class DimRegion implements Serializable {
    private static final long serialVersionUID = 1L;
    private String regionId;
    private String regionName;
    private String parentRegionId;
    private String regionLevel;
}
