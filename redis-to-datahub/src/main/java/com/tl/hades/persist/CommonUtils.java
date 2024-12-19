package com.tl.hades.persist;

import com.ls.athena.framew.terminal.archivesmanager.Protocol3761ArchivesObject;
import com.ls.athena.framew.terminal.archivesmanager.TerminalArchives;
import com.ls.athena.framew.terminal.archivesmanager.TerminalArchivesObject;
import com.tl.dataprocess.param.ParamConstants;
import com.tl.easb.utils.DateUtil;
import com.tl.hades.datahub.DataHubTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

public class CommonUtils {


    private static Logger logger = LoggerFactory.getLogger(CommonUtils.class);

    /**
     * @param ac
     * @param ta
     * @param maddr
     * @return
     */
    public static boolean checkArchhives(String ac, String ta, String maddr) {


        return true;
    }


    /**
     * TerminalId
     * mpedIdStr
     * 数据召测时间
     * businessDataitemId
     * protocolId
     *
     * @param objects
     * @return
     */
    public static Object[] refreshKey(Object... objects) {
        Object[] refreshKey = new Object[5];
        for (int i = 0; i < 5; i++) {
            refreshKey[i] = objects[i];
        }
        return refreshKey;
    }


    /**
     * 面向对象先查询出真实表地址再查询档案
     *
     * @Author Dong.wei-CHEN
     * Date 2019/11/1 8:47
     * @Descrintion 查询档案 ，面向对象档案不再需要去获取真实表地址
     */
    public static TerminalArchivesObject getTerminalArchives(String areaCode, String termAddr, String meterAddr) {

        //获取前置写入redis中的真实表地址
//        String realAddr = TerminalArchives.getInstance().getRealMeterAddrFromLocalOop(areaCode, termAddr, meterAddr);
        //拼接终端地址用于后续查找档案
//        String full = areaCode + termAddr;
//        String realAc1 = full.substring(3, 7);
//        String realTd1 = full.substring(7, 12);
//        String realAc = String.valueOf(Integer.parseInt(realAc1));
//        String realTd = String.valueOf(Integer.parseInt(realTd1));
//        String realAc = areaCode;
//        String realTd = termAddr;
        String realAc = areaCode;
        String realTd = termAddr;
        if (ParamConstants.startWith.equals("41")) {
            String full = areaCode + termAddr;
            String realAc1 = full.substring(3, 7);
            String realTd1 = full.substring(7, 12);
            realAc = String.valueOf(Integer.parseInt(realAc1));
            realTd = String.valueOf(Integer.parseInt(realTd1));
        }
        TerminalArchivesObject terminalArchivesObject = null;
        if (meterAddr != null) {
            //获取档案信息
            terminalArchivesObject = TerminalArchives.getInstance().getTmnlAndMMidArchives(areaCode, termAddr, "COMMADDR" + meterAddr, "MI" + meterAddr);
        } else {
            terminalArchivesObject = TerminalArchives.getInstance().getTmnlArchives(areaCode, termAddr);
        }

        return terminalArchivesObject;
    }

    /**
     * 添加到datahub
     *
     * @param businessDataitemId
     * @param indexKey
     * @param dataListFinal
     * @param refreshKey
     * @param listDataObj
     * @throws Exception
     */
    public static void putToDataHub(String businessDataitemId, String indexKey, List<Object> dataListFinal, Object[] refreshKey, List<DataObject> listDataObj) throws Exception {
        DataHubTopic dataHubTopic = new DataHubTopic(businessDataitemId);
        int index = Math.abs(indexKey.hashCode()) % dataHubTopic.getShardCount();
        String shardId = dataHubTopic.getActiveShardList().get(index);
        DataObject dataObj = new DataObject(dataListFinal, refreshKey, dataHubTopic.topic(), shardId);//给这个数据类赋值:list key classname (fn改为161)
        listDataObj.add(dataObj);//将组好的数据类添加到数据列表
    }

    public static TerminalArchivesObject getArchives(String areaCode, int terminalAddr, String meterAddr) {
        TerminalArchivesObject terminalArchivesObject = TerminalArchives.getInstance().getTmnlAndMMidArchives(areaCode, String.valueOf(terminalAddr), "COMMADDR" + meterAddr, "MI" + meterAddr);
        String mpedIdStr = terminalArchivesObject.getID();
        if (null == mpedIdStr || "".equals(mpedIdStr)) {
            logger.error("无法从缓存获取正确的档案信息:" + areaCode + "_" + terminalAddr);
            return terminalArchivesObject;
        }
        return terminalArchivesObject;
    }


    public static Protocol3761ArchivesObject getProArchive(int protocolId, int afn, int fn) {
        Protocol3761ArchivesObject protocol3761ArchivesObject = new Protocol3761ArchivesObject(protocolId);
        protocol3761ArchivesObject.setAfn(afn);
        protocol3761ArchivesObject.setFn(fn);
        return protocol3761ArchivesObject;
    }

    /**
     * 日期格式化
     * @param d
     * @return
     * @throws ParseException
     */
    public static Date changeDate(String d) throws ParseException {
        return DateUtil.parse(d);
    }



    private static ThreadLocal<Calendar> currentCalendar = new ThreadLocal<Calendar>();
    private static ThreadLocal<SimpleDateFormat> currentFormat = new ThreadLocal<SimpleDateFormat>();
    public static Calendar getCalendar() {
        Calendar calendar = currentCalendar.get();
        if (null == calendar) {
            calendar = Calendar.getInstance();
            currentCalendar.set(calendar);
        }
        return calendar;
    }

    public  static SimpleDateFormat getFormat() {
        SimpleDateFormat format = currentFormat.get();
        if (null == format) {
            format = new SimpleDateFormat("yyyy-MM-dd");
            currentFormat.set(format);
        }
        return format;
    }

    /**
     * allnull判断
     * @param list
     * @return
     */
    @SuppressWarnings("rawtypes")
    public static boolean allNull(List list) { //传入了当前pnfn测量点的数据项列表list
        boolean allNull = true;
        for (Object o : list) {
            if (o instanceof List) {
                allNull((List) o);
            } else {
                if (o != null && !(o instanceof Date) && !(o instanceof Integer)) {
                    allNull = false;
                    return allNull;
                }
            }
        }
        return allNull;
    }

    @SuppressWarnings("rawtypes")
    public static boolean isHistory(List dataList) {
        boolean isHistory = true;
        if (dataList != null && dataList.size() > 0) {
            if (dataList.get(0) instanceof Date) {
                Date requireDate = (Date) dataList.get(0);
                Calendar cal = Calendar.getInstance();
                cal.setTime(new Date());
                cal.add(Calendar.DATE, -1);
                System.out.println(requireDate.compareTo(cal.getTime()));
                String ori = new SimpleDateFormat("yyyy-MM-dd").format(requireDate);
                String yes = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime());
                if (ori.equals(yes)) {
                    isHistory = false;
                }
            }
        }
        return isHistory;
    }


    /**
     * 某月第一天
     * @param score 上几个月 如：score=2  就是上两个月
     * @return
     * @throws Exception
     */
    public static String lastDayByMonth(int score) throws Exception {
        Calendar first = Calendar.getInstance();
        first.setTime(new Date());
        Calendar last = Calendar.getInstance();
        last.set(Calendar.YEAR, first.get(Calendar.YEAR));
        last.add(Calendar.MONTH, -score);
        last.set(Calendar.DAY_OF_MONTH, 1);
        return DateUtil.format(last.getTime(), DateUtil.defaultDatePattern_YMD);
    }

    public static void main(String[] args) throws Exception {
        System.out.println(lastDayByMonth(1));
    }


    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    /**
     * 需量时间格式化
     *
     * @param d
     * @return
     * @throws ParseException
     */
    public static Date demandDateFormat(Object d) throws ParseException {
        Date demandDate = null;
        if (d != null) {
            try {
                demandDate = sdf.parse(d + ":00");
            } catch (Exception e) {
                StringBuffer sb = new StringBuffer();
                sb.append("20");
                sb.append(d);
                sb.append(":00");
                demandDate = sdf.parse(sb.toString());
            }
        }
        return demandDate;
    }
}
