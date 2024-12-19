package com.datasource.impl;

import com.datasource.DataSourceObject;
import com.datasource.DataSourceControl;
import com.datasource.InitDis;
import com.datasource.util.Utils;
import com.huaweicloud.dis.iface.data.request.PutRecordsRequest;
import com.huaweicloud.dis.iface.data.request.PutRecordsRequestEntry;
import com.huaweicloud.dis.iface.data.response.PutRecordsResult;
import com.huaweicloud.dis.iface.data.response.PutRecordsResultEntry;
import com.tl.dataprocess.refreshkey.RefreshDataCache;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;


public class DataSourceToDis implements DataSourceControl {

    Logger logger = Logger.getLogger(DataSourceControl.class);

    @Autowired
    private RefreshDataCache refreshprocessor;
    private static AtomicInteger ato = new AtomicInteger(0);
    private static long time = System.currentTimeMillis();

    @Override
    public void writeToDataSource(List<DataSourceObject> dataSourceObject) {
        if (dataSourceObject == null || dataSourceObject.isEmpty()) {
            return;
        }
        long start=System.currentTimeMillis();
        Map<String, PutRecordsRequest> putRecordsMap = new HashMap<>(16);
        List<Object[]> refreshKeyList = new ArrayList<>();
        dataSourceObject.forEach((dso) -> {
            ato.getAndIncrement();
            String streamName = dso.getTopicName();
            PutRecordsRequest putRecordsRequest = putRecordsMap.get(streamName);
            Object[] object = dso.getRefreshKey();
            List<Object> dataList = dso.getDataList();
            List<PutRecordsRequestEntry> putRecordsRequestEntryList;
            if (putRecordsRequest == null) {
                putRecordsRequest = new PutRecordsRequest();
                putRecordsRequest.setStreamName(streamName);
                putRecordsRequestEntryList = new ArrayList<>();
                putRecordsRequest.setRecords(putRecordsRequestEntryList);
                putRecordsMap.put(streamName, putRecordsRequest);
            } else {
                putRecordsRequestEntryList = putRecordsRequest.getRecords();
            }
            StringBuffer sb = Utils.getPutRecords(dataList);
            ByteBuffer buffer = ByteBuffer.wrap(sb.toString().getBytes());
            PutRecordsRequestEntry putRecordsRequestEntry = new PutRecordsRequestEntry();
            putRecordsRequestEntry.setData(buffer);
            putRecordsRequestEntry.setPartitionKey(String.valueOf(ThreadLocalRandom.current().nextInt(1000000)));
            putRecordsRequestEntryList.add(putRecordsRequestEntry);
            if (object != null)
                refreshKeyList.add(object);
        });
        try {
            putRecordsMap.forEach((k, v) -> {
                PutRecordsResult putRecordsResult = InitDis.getInstance().putRecords(v);
                if (putRecordsResult != null) {

                    for (int j = 0, s = putRecordsResult.getRecords().size(); j < s; j++) {
                        PutRecordsResultEntry putRecordsRequestEntry = putRecordsResult.getRecords().get(j);
                        if (putRecordsRequestEntry.getErrorCode() != null) {
                            // 上传失败
                            logger.info("Error write to dis project topic " + k + "  " + putRecordsRequestEntry.getErrorMessage());
                        } else {
                            // 上传成功
                            logger.info("You have a new message write to dis project topic " + k + " and " + putRecordsRequestEntry.getPartitionId());
                        }
                    }
                }
            });
        } catch (Exception e) {
            logger.info(e);
        }
        if (!refreshKeyList.isEmpty())
            //刷新缓存
            Utils.doRefresh(refreshKeyList, refreshprocessor);
        logger.info(ato.get()+"      "+(System.currentTimeMillis()-time)+"  "+start+" "+System.currentTimeMillis());
    }
}
