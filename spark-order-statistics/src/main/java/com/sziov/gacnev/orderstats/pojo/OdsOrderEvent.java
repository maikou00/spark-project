package com.sziov.gacnev.orderstats.pojo;

import lombok.Data;
import java.io.Serializable;

@Data
public class OdsOrderEvent implements Serializable {
    private static final long serialVersionUID = 1L;
    private String eventId;
    private String eventType;
    private String eventData;
    private String eventTime;
    private String dt;
}
