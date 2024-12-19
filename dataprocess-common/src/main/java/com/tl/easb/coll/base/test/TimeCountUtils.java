package com.tl.easb.coll.base.test;

import com.tl.easb.coll.base.utils.TrackInfoUtils;

public class TimeCountUtils {
    public static String log(long usedTime, long num) {
        return "[" + TrackInfoUtils.getCurrentMethod(3) + "]," + mkLogInfo(usedTime, num);
    }

    public static String log(String executor, long usedTime, long num) {
        return "[" + executor + "]," + mkLogInfo(usedTime, num);
    }

    private static String mkLogInfo(long usedTime, long num) {
        return "counted:" + num + ",used time[ms]:" + usedTime + ",[second]" + usedTime / 1000 + ",numbers per ms:" + num / usedTime + ",numbers per second:" + num / usedTime * 1000;
    }

    public static String log(int traceInfoIdx, long usedTime, long num) {
        return "[" + TrackInfoUtils.getCurrentMethod(traceInfoIdx) + "]," + mkLogInfo(usedTime, num);
    }
}
