package com.datasource.util;

import com.tl.dataprocess.refreshkey.RefreshDataCache;

import java.util.Date;
import java.util.List;

public class Utils {
    public static StringBuffer getPutRecords(List<Object> objects) {

        StringBuffer sb = new StringBuffer();
        objects.forEach((item) -> {
            if (item == null) {
                sb.append(",");
            } else {
                if (item instanceof Date) {
                    sb.append(((Date) item).getTime() + ",");
                } else {
                    sb.append(item + ",");
                }
            }
        });
        sb.deleteCharAt(sb.length() - 1);
        return sb;
    }

    public static void doRefresh(List<Object[]> refreshKeyArrList, RefreshDataCache refreshprocessor) {
        if (refreshKeyArrList.size() == 0) {
            return;
        }
        refreshKeyArrList.forEach((item) -> {
            if (null != item)
                refreshprocessor.refresh(item);
        });
    }
}
