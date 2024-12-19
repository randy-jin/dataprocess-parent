package com.datasource;

import com.ls.athena.framework.sqlconfig.SG3761SqlMapping;

import java.util.List;

/**
 * @author Dongwei-Chen
 * @Date 2020/2/21 11:50
 * @Description 传入的对象
 */
public class DataSourceObject {

    private String topicName;

    private Object[] refreshKey;

    private List<Object> dataList;

    public DataSourceObject(String businessItemId, Object[] refreshKey, List<Object> dataList) {
        this.topicName = SG3761SqlMapping.getInstance().getTopicName(businessItemId);
        this.refreshKey = refreshKey;
        this.dataList = dataList;
    }

    public String getTopicName() {
        return topicName;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    public Object[] getRefreshKey() {
        return refreshKey;
    }

    public void setRefreshKey(Object[] refreshKey) {
        this.refreshKey = refreshKey;
    }

    public List<Object> getDataList() {
        return dataList;
    }

    public void setDataList(List<Object> dataList) {
        this.dataList = dataList;
    }
}
