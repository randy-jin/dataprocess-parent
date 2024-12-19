package com.tl.easb.utils.CallSend;

import java.io.Serializable;
import java.util.List;

/**
 * Created by huangchunhuai on 2021/9/6.
 */
public class MpedDataSet_bak implements Serializable {
    private String mpedId;
    private String mpedType;
    private String dataItemId;
    private boolean success = true;
    private List extraList = null;
    private String meteType = "";
    private String afn = "";
    private String fn = "";
    private String pn = "";
    private String mpedAddress = "";
    private String ctrl = "";
    private String DI = "";
    private String port = "";
    private String baud = "";
    private String stopBit = "";
    private String NocheckOut = "";
    private String checkOut = "";
    private String number = "";
    private String timeOutUnit = "";
    private String timeOut = "";
    private String timeOutDelay = "";

    public MpedDataSet_bak() {
    }

    public MpedDataSet_bak(String mpedId, String dataItemId) {
        this.mpedId = mpedId;
        this.dataItemId = dataItemId;
    }

    public String getMpedId() {
        return this.mpedId;
    }

    public void setMpedId(String mpedId) {
        this.mpedId = mpedId;
    }

    public String getMpedType() {
        return this.mpedType;
    }

    public void setMpedType(String mpedType) {
        this.mpedType = mpedType;
    }

    public String getDataItemId() {
        return this.dataItemId;
    }

    public void setDataItemId(String dataItemId) {
        this.dataItemId = dataItemId;
    }

    public boolean isSuccess() {
        return this.success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public List getExtraList() {
        return this.extraList;
    }

    public void setExtraList(List extraList) {
        this.extraList = extraList;
    }

    public String getMeteType() {
        return this.meteType;
    }

    public void setMeteType(String meteType) {
        this.meteType = meteType;
    }

    public String getAfn() {
        return this.afn;
    }

    public void setAfn(String afn) {
        this.afn = afn;
    }

    public String getFn() {
        return this.fn;
    }

    public void setFn(String fn) {
        this.fn = fn;
    }

    public String getPn() {
        return this.pn;
    }

    public void setPn(String pn) {
        this.pn = pn;
    }

    public String getMpedAddress() {
        return this.mpedAddress;
    }

    public void setMpedAddress(String mpedAddress) {
        this.mpedAddress = mpedAddress;
    }

    public String getCtrl() {
        return this.ctrl;
    }

    public void setCtrl(String ctrl) {
        this.ctrl = ctrl;
    }

    public String getDI() {
        return this.DI;
    }

    public void setDI(String dI) {
        this.DI = dI;
    }

    public String getPort() {
        return this.port;
    }

    public void setPort(String port) {
        this.port = port;
    }

    public String getBaud() {
        return this.baud;
    }

    public void setBaud(String baud) {
        this.baud = baud;
    }

    public String getStopBit() {
        return this.stopBit;
    }

    public void setStopBit(String stopBit) {
        this.stopBit = stopBit;
    }

    public String getNocheckOut() {
        return this.NocheckOut;
    }

    public void setNocheckOut(String nocheckOut) {
        this.NocheckOut = nocheckOut;
    }

    public String getCheckOut() {
        return this.checkOut;
    }

    public void setCheckOut(String checkOut) {
        this.checkOut = checkOut;
    }

    public String getNumber() {
        return this.number;
    }

    public void setNumber(String number) {
        this.number = number;
    }

    public String getTimeOutUnit() {
        return this.timeOutUnit;
    }

    public void setTimeOutUnit(String timeOutUnit) {
        this.timeOutUnit = timeOutUnit;
    }

    public String getTimeOut() {
        return this.timeOut;
    }

    public void setTimeOut(String timeOut) {
        this.timeOut = timeOut;
    }

    public String getTimeOutDelay() {
        return this.timeOutDelay;
    }

    public void setTimeOutDelay(String timeOutDelay) {
        this.timeOutDelay = timeOutDelay;
    }
}
