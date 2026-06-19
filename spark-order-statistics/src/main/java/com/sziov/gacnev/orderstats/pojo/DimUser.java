package com.sziov.gacnev.orderstats.pojo;

import lombok.Data;
import java.io.Serializable;

@Data
public class DimUser implements Serializable {
    private static final long serialVersionUID = 1L;
    private String userId;
    private String userName;
    private String phone;
    private String email;
    private String regionId;
    private String registerDate;
}
