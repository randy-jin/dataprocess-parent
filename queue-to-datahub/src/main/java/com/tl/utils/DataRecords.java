package com.tl.utils;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;

import java.util.List;
import java.util.Map;

/**
 * Created by huangchunhuai on 2021/12/11.
 */
@Data
@Getter
@Setter
public class DataRecords {
    private String terminalId;
    private String protocolId;
    private String areaCode;
    private String terminalAddr;
    private String meterId;
    private String commAddr;
    private String dataItemId;
    private String frameId;
    private String tableName;
    private Map<String, List<Object>> datas;
}
