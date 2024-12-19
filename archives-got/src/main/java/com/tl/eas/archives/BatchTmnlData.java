package com.tl.eas.archives;

import java.util.List;

public class BatchTmnlData {
    private String appNo;
    private String shardNo;
    private List<String> batchTmnlInfo;

    public String getAppNo() {
        return appNo;
    }

    public void setAppNo(String appNo) {
        this.appNo = appNo;
    }

    public String getShardNo() {
        return shardNo;
    }

    public void setShardNo(String shardNo) {
        this.shardNo = shardNo;
    }

    public List<String> getBatchTmnlInfo() {
        return batchTmnlInfo;
    }

    public void setBatchTmnlInfo(List<String> batchTmnlInfo) {
        this.batchTmnlInfo = batchTmnlInfo;
    }
}