package com.datasource;

import java.util.List;

/**
 * @author Dongwei-Chen
 * @Date 2020/2/21 11:47
 * @Description
 */
public interface DataSourceControl {

    void writeToDataSource(List<DataSourceObject> dataSourceObject);

}
