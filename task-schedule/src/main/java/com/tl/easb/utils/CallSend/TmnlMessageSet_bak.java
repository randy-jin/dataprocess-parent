package com.tl.easb.utils.CallSend;


import java.io.Serializable;
import java.util.List;

/**
 * Created by huangchunhuai on 2021/9/6.
 */
public class TmnlMessageSet_bak implements Serializable {
    private String tmnlId;
    private String protocolId;
    private String dataFlag;
    private String dataObj;
    private int sendTimeout = 0;
    private int responseTimeout = 60;
    private String password;
    private int priority = 0;
    private boolean writeHbase = false;
    private boolean writeDatabase = false;
    private List<MpedDataSet_bak> mpedDataList;
    private String areaCode = "";
    private String tmnlAddr = "";
    private String tmnlTaskId;

    public String getTmnlTaskId() {
        return this.tmnlTaskId;
    }

    public void setTmnlTaskId(String tmnlTaskId) {
        this.tmnlTaskId = tmnlTaskId;
    }

    public TmnlMessageSet_bak() {
    }

    public String getAreaCode() {
        return this.areaCode;
    }

    public void setAreaCode(String areaCode) {
        this.areaCode = areaCode;
    }

    public String getTmnlAddr() {
        return this.tmnlAddr;
    }

    public void setTmnlAddr(String tmnlAddr) {
        this.tmnlAddr = tmnlAddr;
    }

    public TmnlMessageSet_bak(String tmnlId, String dataFlag) {
        this.tmnlId = tmnlId;
        this.dataFlag = dataFlag;
        this.writeDatabase = true;
    }

    public String getTmnlId() {
        return this.tmnlId;
    }

    public void setTmnlId(String tmnlId) {
        this.tmnlId = tmnlId;
    }

    public String getProtocolId() {
        return this.protocolId;
    }

    public void setProtocolId(String protocolId) {
        this.protocolId = protocolId;
    }

    public String getDataFlag() {
        return this.dataFlag;
    }

    public void setDataFlag(String dataFlag) {
        this.dataFlag = dataFlag;
    }

    public String getDataObj() {
        return this.dataObj;
    }

    public void setDataObj(String dataObj) {
        this.dataObj = dataObj;
    }

    public int getSendTimeout() {
        return this.sendTimeout;
    }

    public void setSendTimeout(int sendTimeout) {
        this.sendTimeout = sendTimeout;
    }

    public int getResponseTimeout() {
        return this.responseTimeout;
    }

    public void setResponseTimeout(int responseTimeout) {
        this.responseTimeout = responseTimeout;
    }

    public String getPassword() {
        return this.password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public int getPriority() {
        return this.priority;
    }

    public void setPriority(int priority) {
        this.priority = priority;
    }

    public boolean isWriteHbase() {
        return this.writeHbase;
    }

    public void setWriteHbase(boolean writeHbase) {
        this.writeHbase = writeHbase;
    }

    public boolean isWriteDatabase() {
        return this.writeDatabase;
    }

    public void setWriteDatabase(boolean writeDatabase) {
        this.writeDatabase = writeDatabase;
    }

    public List<MpedDataSet_bak> getMpedDataList() {
        return this.mpedDataList;
    }

    public void setMpedDataList(List<MpedDataSet_bak> mpedDataList) {
        this.mpedDataList = mpedDataList;
    }
}
