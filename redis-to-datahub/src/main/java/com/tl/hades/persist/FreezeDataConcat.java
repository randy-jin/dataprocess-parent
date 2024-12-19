package com.tl.hades.persist;

import com.ls.athena.dataprocess.sg3761.beans.DataItemObject;
import com.ls.athena.dataprocess.sg3761.beans.TerminalDataObject;
import com.ls.athena.framew.terminal.archivesmanager.Protocol3761ArchivesObject;
import com.ls.athena.framew.terminal.archivesmanager.ProtocolArchives;
import com.ls.athena.framew.terminal.archivesmanager.TerminalArchives;
import com.ls.athena.framew.terminal.archivesmanager.TerminalArchivesObject;
import com.tl.dataprocess.param.ParamConstants;
import com.tl.easb.utils.DateUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author Dongwei-Chen
 * @Date 2020/5/27 9:14
 * @Description 日冻结组装
 */
public class FreezeDataConcat {

    private static Logger logger = LoggerFactory.getLogger(FreezeDataConcat.class);

    private static Pattern r = Pattern.compile("(\\d+(\\.\\d+)?)");

    private static SimpleDateFormat yearAndMon = new SimpleDateFormat("yyyy-MM");

    private static SimpleDateFormat dayAndHours = new SimpleDateFormat("-dd HH:mm:ss");

    private static SimpleDateFormat parseDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static void getDataList(TerminalDataObject terminalDataObject, int protocolId, List<DataObject> listDataObj, DataItemObject data, String sType) throws Exception {
        List dataList = data.getList();
        if (dataList.size() == 0) {
            return;
        }
        int afn = terminalDataObject.getAFN();
        int fn = data.getFn();
        int pn = data.getPn();
        Date clock;
        String areaCode = terminalDataObject.getAreaCode();
        int terminalAddr = terminalDataObject.getTerminalAddr();
        logger.info("===MAKE " + afn + "_" + fn + " DATA===");

        Protocol3761ArchivesObject protocol3761ArchivesObject = CommonUtils.getProArchive(protocolId, afn, fn);

        String businessDataitemId = ProtocolArchives.getInstance().getProtocolArchivesObject(protocol3761ArchivesObject).getBusiDataItemId();

        String afnAndfn = afn + "#" + fn;
        TerminalArchivesObject tao = TerminalArchives.getInstance().getTmnlAndMpedIdArchives(areaCode, String.valueOf(terminalAddr), "P" + pn);
        Object[] refreshKey = null;
        String data_src;
        String terminalId = tao.getTerminalId();
        String mpedIdStr = tao.getID();
        List finalDataList = new ArrayList();
        if (mpedIdStr == null || "null".equals(mpedIdStr)) {
            return;
        }
        switch (afnAndfn) {
            //预抄日冻结正向有无功
            case "13#1":
            case "13#2":
                int typeFlag;
                if (fn == 1) {
                    typeFlag = 1;
                } else {
                    typeFlag = 5;
                }
                List<List> appendList = new ArrayList<>();
                for (int i = 3; i < dataList.size(); i += 2) {
                    List append = new ArrayList();
                    append.add(typeFlag);
                    append.add(dataList.get(i));
                    List feeList = (List) dataList.get(i + 1);
                    for (Object o : (List) feeList.get(0)
                            ) {
                        append.add(o);
                    }
                    typeFlag++;
                    appendList.add(append);
                }
                for (List append : appendList
                        ) {
                    List finalDatas = new ArrayList();
                    Date date = (Date) dataList.get(0);
                    String dataDate = DateUtil.format(date, "yyyyMMdd");
                    finalDatas.add(mpedIdStr + "_" + dataDate);
                    finalDatas.add(mpedIdStr);

                    finalDatas.add(append.remove(0));
                    finalDatas.add(dataList.get(1));
                    finalDatas.addAll(append);
                    for (int i = 0; i < 10; i++) {
                        finalDatas.add(null);
                    }

                    finalDatas.add(tao.getPowerUnitNumber());//供电单位编号

                    finalDatas.add("00");//未分析
                    //区分直抄8/预抄9
                    if ("auto".equals(sType)) {
                        data_src = "0";
                    } else {
                        data_src = "4";
                    }
                    finalDatas.add(data_src);//预抄
                    finalDatas.add(new Date());//入库时间
                    finalDatas.add(tao.getPowerUnitNumber().substring(0, 5));//前闭后开  前5位 不包括[5]
                    finalDatas.add(DateUtil.format(date, DateUtil.defaultDatePattern_YMD)); //日冻结类数据时标Td_d(数据日期)
                    finalDataList.add(finalDatas);
                }

                refreshKey = CommonUtils.refreshKey(tao.getTerminalId(), mpedIdStr, dataList.get(0), businessDataitemId, protocolId);

                break;
            //分支箱正向有功
            case "13#139":
                List dataLists = (List) dataList.get(2);
                for (int i = 0; i < dataLists.size(); i++) {
                    List meterList = (List) dataLists.get(i);
                    String maddr = (String) meterList.get(1);
                    TerminalArchivesObject terminalArchivesObject;
                    if (ParamConstants.startWith.equals("11")) {
                        terminalArchivesObject = TerminalArchives.getInstance().getTmnlAndMMidArchives(areaCode, String.valueOf(terminalAddr), "COMMADDR" + maddr, "MI" + maddr);
                    } else {
                        terminalArchivesObject = TerminalArchives.getInstance().getTmnlAndMpedIdArchives(areaCode, String.valueOf(terminalAddr), "COMMADDR" + maddr);
                    }
                    if (terminalArchivesObject == null) {
                        logger.error("无法从缓存获取正确的档案信息:" + areaCode + "_" + terminalAddr);
                        continue;
                    }
                    mpedIdStr = terminalArchivesObject.getID();
                    if (null == mpedIdStr || "".equals(mpedIdStr)) {
                        logger.error("无法从缓存获取正确的档案信息:" + areaCode + "_" + terminalAddr);
                        continue;
                    }
                    finalDataList.add(mpedIdStr);
                    finalDataList.add(dataList.get(1));
                    finalDataList.add(DateUtil.format((Date) dataList.get(0), DateUtil.defaultDatePattern_YMD));//数据时间
                    finalDataList.add(meterList.get(1));
                    finalDataList.add(meterList.get(0));
                    finalDataList.add(meterList.get(2));
                    finalDataList.add(meterList.get(3));

                    finalDataList.add(terminalArchivesObject.getPowerUnitNumber());//供电单位编号
                    finalDataList.add("00");
                    //区分直抄8/预抄9
                    if ("auto".equals(sType)) {
                        data_src = "0";
                    } else {
                        data_src = "4";
                    }
                    finalDataList.add(data_src);//预抄
                    finalDataList.add(new Date());//入库时间3531778244
                    finalDataList.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));//前闭后开  前5位 不包括[5]
                }
                refreshKey = CommonUtils.refreshKey(tao.getTerminalId(), mpedIdStr, dataList.get(0), businessDataitemId, protocolId);
                break;
            //分支箱小时冻结正向有功
            case "13#140":
                List meterList = (List) ((List) dataList.get(4)).get(0);
                String maddr = (String) meterList.get(1);
                TerminalArchivesObject terminalArchivesObject;
                if (ParamConstants.startWith.equals("11")) {
                    terminalArchivesObject = TerminalArchives.getInstance().getTmnlAndMMidArchives(areaCode, String.valueOf(terminalAddr), "COMMADDR" + maddr, "MI" + maddr);
                } else {
                    terminalArchivesObject = TerminalArchives.getInstance().getTmnlAndMpedIdArchives(areaCode, String.valueOf(terminalAddr), "COMMADDR" + maddr);
                }
                if (terminalArchivesObject == null) {
                    logger.error("无法从缓存获取正确的档案信息:" + areaCode + "_" + terminalAddr);
                    return;
                }
                mpedIdStr = terminalArchivesObject.getID();
                if (null == mpedIdStr || "".equals(mpedIdStr)) {
                    logger.error("无法从缓存获取正确的档案信息:" + areaCode + "_" + terminalAddr);
                    return;
                }
                finalDataList.add(mpedIdStr);
                finalDataList.add(dataList.get(3));//数据类型
                finalDataList.add(FzxUtils.dataTime(String.valueOf(dataList.get(1)), (Date) dataList.get(0)));//数据时间类型
                finalDataList.add(DateUtil.format((Date) dataList.get(0), DateUtil.defaultDatePattern_YMD));//数据时间
                finalDataList.add(dataList.get(1));//数据点标志
                finalDataList.add(meterList.get(1));
                finalDataList.add(meterList.get(0));
                finalDataList.add(meterList.get(2));
                finalDataList.add(meterList.get(3));

                finalDataList.add(terminalArchivesObject.getPowerUnitNumber());//供电单位编号
                finalDataList.add("00");
                //区分直抄8/预抄9
                if ("auto".equals(sType)) {
                    data_src = "0";
                } else {
                    data_src = "4";
                }
                finalDataList.add(data_src);//预抄
                finalDataList.add(new Date());//入库时间3531778244
                finalDataList.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));//前闭后开  前5位 不包括[5]
                refreshKey = CommonUtils.refreshKey(tao.getTerminalId(), mpedIdStr, dataList.get(0), businessDataitemId, protocolId);
                break;
            //日功率因数区段累计时间
            case "13#43":
                finalDataList.add(mpedIdStr);
                for (int i = 1; i < 4; i++) {
                    finalDataList.add(dataList.get(i));
                }
                finalDataList.add(tao.getPowerUnitNumber());//供电单位编号
                finalDataList.add("00");//未分析
                //区分直抄8/预抄9
                if ("auto".equals(sType)) {
                    data_src = "0";
                } else {
                    data_src = "4";
                }
                finalDataList.add(data_src);//预抄
                finalDataList.add(new Date());//入库时间
                finalDataList.add(tao.getPowerUnitNumber().substring(0, 5));//前闭后开  前5位 不包括[5]
                // 把数据时标放在最后，方便外层处理
                Object dataDate = dataList.get(0);
                finalDataList.add(DateUtil.format((Date) dataDate, DateUtil.defaultDatePattern_YMD)); //日冻结类数据时标Td_d(数据日期)
                refreshKey = CommonUtils.refreshKey(tao.getTerminalId(), mpedIdStr, dataList.get(0), businessDataitemId, protocolId);
                break;
            //正/反向有功/无功需量
            case "13#3":
            case "13#4":
            case "13#19":
            case "13#20":
                if (!"41".equals(ParamConstants.startWith)) {
                    for (int i = 1; i < 3; i++) {
                        finalDataList = new ArrayList();
                        finalDataList.add(mpedIdStr);
                        finalDataList.add(i);
                        finalDataList.add(dataList.get(1));
                        Object obj_t = dataList.get(4 * i + 1);
                        Object obj_v = dataList.get((1 + i) * i + 1);
                        if (obj_v == null) {
                            continue;
                        }
                        finalDataList.add(obj_v);

                        Calendar calDate = Calendar.getInstance();
                        Calendar calDataDate = Calendar.getInstance();
                        calDataDate.setTime((Date) dataList.get(1));
                        if (obj_t != null) {
                            calDate.setTime((Date) obj_t);
                            calDate.set(Calendar.YEAR, calDataDate.get(Calendar.YEAR));
                            obj_t = calDate.getTime();
                        }
                        finalDataList.add(obj_t);

                        finalDataList.add(tao.getPowerUnitNumber());//供电单位编号
                        finalDataList.add("00");//未分析
                        finalDataList.add("0");//自动抄表
                        finalDataList.add(new Date());//入库时间
                        finalDataList.add(tao.getPowerUnitNumber().substring(0, 5));//前闭后开  前5位 不包括[5]
                        finalDataList.add(DateUtil.format((Date) dataList.get(0), DateUtil.defaultDatePattern_YMD));//数据时间

                        refreshKey = CommonUtils.refreshKey(tao.getTerminalId(), mpedIdStr, dataList.get(0), businessDataitemId, protocolId);
                    }
                    break;
                }
                int fNum = 4;
                int tNum = 6;
                String type = "1";
                if (fn == 20 || fn == 4) {
                    type = "5";
                }
                for (int j = 0; j < 2; j++) {
                    List newDataList = new ArrayList();
                    clock = (Date) dataList.get(0);

                    newDataList.add(mpedIdStr);
                    newDataList.add(type);
                    newDataList.add(clock);

                    Object ov = dataList.get(fNum - 1);
                    if (ov == null) {
                        continue;
                    }
                    newDataList.add(ov);
                    newDataList.add(dataList.get(tNum - 1));
                    List fee = (List) ((List) dataList.get(fNum)).get(0);
                    List time = (List) ((List) dataList.get(tNum)).get(0);
                    for (int i = 0; i < fee.size(); i++) {
                        newDataList.add(fee.get(i));
                        newDataList.add(time.get(i));
                    }
                    newDataList.add(DateUtil.format(clock, DateUtil.defaultDatePattern_YMD));
                    newDataList.set(2, terminalDataObject.getUpTime());
                    newDataList.add(tao.getPowerUnitNumber());//供电单位编号
                    newDataList.add("00");//未分析
                    //区分直抄8/预抄9
                    if ("auto".equals(sType)) {
                        data_src = "0";
                    } else {
                        data_src = "4";
                    }
                    newDataList.add(data_src);//预抄
                    newDataList.add(new Date());//入库时间
                    newDataList.add(tao.getPowerUnitNumber().substring(0, 5));
                    newDataList.add(newDataList.size(), newDataList.get(newDataList.size() - 6));
                    newDataList.remove(newDataList.size() - 7);
                    fNum = 8;
                    tNum = 10;
                    type = "2";
                    if (fn == 20 || fn == 4) {
                        type = "6";
                    }
                    finalDataList.add(newDataList);
                }
                refreshKey = CommonUtils.refreshKey(tao.getTerminalId(), mpedIdStr, dataList.get(0), businessDataitemId, protocolId);
                break;
            //日总及分相最大有功功率及发生时间、有功功率为零时间
            case "13#25":
                finalDataList.add(mpedIdStr);

                Calendar calDate = Calendar.getInstance();
                Calendar calDataDate = Calendar.getInstance();
                calDataDate.setTime((Date) dataList.get(0));
                Object obj;

                finalDataList.add(dataList.get(1));
                obj = dataList.get(2);
                if (obj != null) {
                    calDate.setTime((Date) obj);
                    calDate.set(Calendar.YEAR, calDataDate.get(Calendar.YEAR));
                    calDate.set(Calendar.MONTH, calDataDate.get(Calendar.MONTH));
                    obj = calDate.getTime();
                } else {
                    obj = null;
                }
                finalDataList.add(obj);
                finalDataList.add(dataList.get(3));
                obj = dataList.get(4);
                if (obj != null) {
                    calDate.setTime((Date) obj);
                    calDate.set(Calendar.YEAR, calDataDate.get(Calendar.YEAR));
                    calDate.set(Calendar.MONTH, calDataDate.get(Calendar.MONTH));
                    obj = calDate.getTime();
                } else {
                    obj = null;
                }
                finalDataList.add(obj);
                finalDataList.add(dataList.get(5));
                obj = dataList.get(6);
                if (obj != null) {
                    calDate.setTime((Date) obj);
                    calDate.set(Calendar.YEAR, calDataDate.get(Calendar.YEAR));
                    calDate.set(Calendar.MONTH, calDataDate.get(Calendar.MONTH));
                    obj = calDate.getTime();
                } else {
                    obj = null;
                }
                finalDataList.add(obj);
                finalDataList.add(dataList.get(7));
                obj = dataList.get(8);
                if (obj != null) {
                    calDate.setTime((Date) obj);
                    calDate.set(Calendar.YEAR, calDataDate.get(Calendar.YEAR));
                    calDate.set(Calendar.MONTH, calDataDate.get(Calendar.MONTH));
                    obj = calDate.getTime();
                } else {
                    obj = null;
                }
                finalDataList.add(obj);
                finalDataList.add(tao.getPowerUnitNumber());//供电单位编号
                finalDataList.add("00");//未分析
                //区分直抄8/预抄9
                if ("auto".equals(sType)) {
                    data_src = "0";
                } else {
                    data_src = "4";
                }
                finalDataList.add(data_src);//预抄
                finalDataList.add(new Date());//入库时间
                finalDataList.add(tao.getPowerUnitNumber().substring(0, 5));//前闭后开  前5位 不包括[5]
                dataDate = dataList.get(0);
                finalDataList.add(DateUtil.format((Date) dataDate, DateUtil.defaultDatePattern_YMD)); //日冻结类数据时标Td_d(数据日期)
                refreshKey = CommonUtils.refreshKey(tao.getTerminalId(), mpedIdStr, dataList.get(0), businessDataitemId, protocolId);

                break;
            //日电流越限统计
            case "13#29":
                finalDataList.add(mpedIdStr);
                for (int i = 1; i < 9; i++) {
                    finalDataList.add(dataList.get(i));
                }
                calDate = Calendar.getInstance();
                calDataDate = Calendar.getInstance();
                calDataDate.setTime((Date) dataList.get(0));
                obj = dataList.get(9);
                if (obj != null) {
                    calDate.setTime((Date) obj);
                    calDate.set(Calendar.YEAR, calDataDate.get(Calendar.YEAR));
                    calDate.set(Calendar.MONTH, calDataDate.get(Calendar.MONTH));
                    obj = calDate.getTime();
                }
                finalDataList.add(obj);
                finalDataList.add(dataList.get(10));
                obj = dataList.get(11);
                if (obj != null) {
                    calDate.setTime((Date) obj);
                    calDate.set(Calendar.YEAR, calDataDate.get(Calendar.YEAR));
                    calDate.set(Calendar.MONTH, calDataDate.get(Calendar.MONTH));
                    obj = calDate.getTime();
                }
                finalDataList.add(obj);
                finalDataList.add(dataList.get(12));
                obj = dataList.get(13);
                if (obj != null) {
                    calDate.setTime((Date) obj);
                    calDate.set(Calendar.YEAR, calDataDate.get(Calendar.YEAR));
                    calDate.set(Calendar.MONTH, calDataDate.get(Calendar.MONTH));
                    obj = calDate.getTime();
                }
                finalDataList.add(obj);
                finalDataList.add(dataList.get(14));
                obj = dataList.get(15);
                if (obj != null) {
                    calDate.setTime((Date) obj);
                    calDate.set(Calendar.YEAR, calDataDate.get(Calendar.YEAR));
                    calDate.set(Calendar.MONTH, calDataDate.get(Calendar.MONTH));
                    obj = calDate.getTime();
                }
                finalDataList.add(obj);

                finalDataList.add(tao.getPowerUnitNumber());//供电单位编号

                finalDataList.add("00");//未分析
                //区分直抄8/预抄9
                if ("auto".equals(sType)) {
                    data_src = "0";
                } else {
                    data_src = "4";
                }
                finalDataList.add(data_src);//预抄
                finalDataList.add(new Date());//入库时间
                finalDataList.add(tao.getPowerUnitNumber().substring(0, 5));//前闭后开  前5位 不包括[5]
                // 把数据时标放在最后，方便外层处理
                dataDate = dataList.get(0);
                finalDataList.add(DateUtil.format((Date) dataDate, DateUtil.defaultDatePattern_YMD)); //日冻结类数据时标Td_d(数据日期)
                refreshKey = CommonUtils.refreshKey(tao.getTerminalId(), mpedIdStr, dataList.get(0), businessDataitemId, protocolId);

                break;
            //yue功率因数区段累计时间
            case "13#44":
                finalDataList.add(mpedIdStr);
                // 把数据时标放在最后，方便外层处理
                dataDate = dataList.get(0);
                if (null != dataDate && dataDate instanceof Date) {
                    finalDataList.add(DateUtil.format((Date) dataDate, DateUtil.defaultDatePattern_YMD)); //日冻结类数据时标Td_d(数据日期)
                }
                for (int i = 1; i < 4; i++) {
                    finalDataList.add(dataList.get(i));
                }
                finalDataList.add(tao.getPowerUnitNumber());//供电单位编号

                finalDataList.add("00");//未分析
                //区分直抄8/预抄9
                if ("auto".equals(sType)) {
                    data_src = "0";
                } else {
                    data_src = "4";
                }
                finalDataList.add(data_src);//预抄
                finalDataList.add(new Date());//入库时间
                finalDataList.add(tao.getPowerUnitNumber().substring(0, 5));//前闭后开  前5位 不包括[5]
                refreshKey = CommonUtils.refreshKey(tao.getTerminalId(), mpedIdStr, dataList.get(0), businessDataitemId, protocolId);

                break;
            case "13#33":
                finalDataList.add(mpedIdStr);
                for (int i = 1; i < 9; i++) {
                    finalDataList.add(dataList.get(1));
                }

                finalDataList.add(tao.getPowerUnitNumber());//供电单位编号

                finalDataList.add("00");//未分析
                //区分直抄8/预抄9
                if ("auto".equals(sType)) {
                    data_src = "0";
                } else {
                    data_src = "4";
                }
                finalDataList.add(data_src);//预抄
                finalDataList.add(new Date());//入库时间
                finalDataList.add(tao.getPowerUnitNumber().substring(0, 5));//前闭后开  前5位 不包括[5]
                // 把数据时标放在最后，方便外层处理
                dataDate = dataList.get(0);
                finalDataList.add(DateUtil.format((Date) dataDate, DateUtil.defaultDatePattern_YMD)); //日冻结类数据时标Td_d(数据日期)
                refreshKey = CommonUtils.refreshKey(tao.getTerminalId(), mpedIdStr, dataList.get(0), businessDataitemId, protocolId);

                break;
            //yue电流越限统计
            case "13#37":
                finalDataList.add(mpedIdStr);
                // 把数据时标放在最后，方便外层处理
                dataDate = dataList.get(0);
                if (null != dataDate && dataDate instanceof Date) {
                    finalDataList.add(DateUtil.format((Date) dataDate, DateUtil.defaultDatePattern_YMD)); //日冻结类数据时标Td_d(数据日期)
                }
                for (int i = 1; i < 16; i++) {
                    finalDataList.add(dataList.get(i));
                }
                finalDataList.add(tao.getPowerUnitNumber());//供电单位编号

                finalDataList.add("00");//未分析
                //区分直抄8/预抄9
                if ("auto".equals(sType)) {
                    data_src = "0";
                } else {
                    data_src = "4";
                }
                finalDataList.add(data_src);//预抄
                finalDataList.add(new Date());//入库时间
                finalDataList.add(tao.getPowerUnitNumber().substring(0, 5));//前闭后开  前5位 不包括[5]
                refreshKey = CommonUtils.refreshKey(tao.getTerminalId(), mpedIdStr, dataList.get(0), businessDataitemId, protocolId);

                break;
            //日电压统计记录
            case "13#27":
                Calendar calendar = Calendar.getInstance();
                calDate = Calendar.getInstance();
                calDate.setTime((Date) dataList.get(0));

                finalDataList.add(mpedIdStr);
                for (int i = 1; i < 17; i++) {
                    finalDataList.add(dataList.get(i));
                }
                if (dataList.get(17) != null) {
                    calendar.setTime((Date) dataList.get(17));//2018-01-01 //2018-06-06
                    calendar.set(Calendar.YEAR, calDate.get(Calendar.YEAR));
                    calendar.set(Calendar.MONTH, calDate.get(Calendar.MONTH));
                    finalDataList.add(calendar.getTime());
                } else {
                    finalDataList.add(dataList.get(17));
                }
                finalDataList.add(dataList.get(18));
                if (dataList.get(19) != null) {
                    calendar.setTime((Date) dataList.get(19));
                    calendar.set(Calendar.YEAR, calDate.get(Calendar.YEAR));
                    calendar.set(Calendar.MONTH, calDate.get(Calendar.MONTH));
                    finalDataList.add(calendar.getTime());
                } else {
                    finalDataList.add(dataList.get(19));
                }
                finalDataList.add(dataList.get(20));
                if (dataList.get(21) != null) {
                    calendar.setTime((Date) dataList.get(21));
                    calendar.set(Calendar.YEAR, calDate.get(Calendar.YEAR));
                    calendar.set(Calendar.MONTH, calDate.get(Calendar.MONTH));
                    finalDataList.add(calendar.getTime());
                } else {
                    finalDataList.add(dataList.get(21));
                }
                finalDataList.add(dataList.get(22));
                if (dataList.get(23) != null) {
                    calendar.setTime((Date) dataList.get(23));
                    calendar.set(Calendar.YEAR, calDate.get(Calendar.YEAR));
                    calendar.set(Calendar.MONTH, calDate.get(Calendar.MONTH));
                    finalDataList.add(calendar.getTime());
                } else {
                    finalDataList.add(dataList.get(23));
                }
                finalDataList.add(dataList.get(24));
                if (dataList.get(25) != null) {
                    calendar.setTime((Date) dataList.get(25));
                    calendar.set(Calendar.YEAR, calDate.get(Calendar.YEAR));
                    calendar.set(Calendar.MONTH, calDate.get(Calendar.MONTH));
                    finalDataList.add(calendar.getTime());
                } else {
                    finalDataList.add(dataList.get(25));
                }
                finalDataList.add(dataList.get(26));
                if (dataList.get(27) != null) {
                    calendar.setTime((Date) dataList.get(27));
                    calendar.set(Calendar.YEAR, calDate.get(Calendar.YEAR));
                    calendar.set(Calendar.MONTH, calDate.get(Calendar.MONTH));
                    finalDataList.add(calendar.getTime());
                } else {
                    finalDataList.add(dataList.get(27));
                }
                finalDataList.add(dataList.get(28));
                finalDataList.add(dataList.get(29));
                finalDataList.add(dataList.get(30));
                if (dataList.size() < 32) {
                    for (int i = 0; i < 9; i++) {
                        finalDataList.add(null);
                    }
                } else {
                    finalDataList.add(dataList.get(31));//a
                    finalDataList.add(dataList.get(34));
                    finalDataList.add(dataList.get(37));

                    finalDataList.add(dataList.get(32));//b
                    finalDataList.add(dataList.get(35));
                    finalDataList.add(dataList.get(38));

                    finalDataList.add(dataList.get(33));//c
                    finalDataList.add(dataList.get(36));
                    finalDataList.add(dataList.get(39));
                }

                finalDataList.add(tao.getPowerUnitNumber());//供电单位编号

                finalDataList.add("00");//未分析
                //区分直抄8/预抄9
                if ("auto".equals(sType)) {
                    data_src = "0";
                } else {
                    data_src = "4";
                }
                finalDataList.add(data_src);//预抄
                finalDataList.add(new Date());//入库时间
                finalDataList.add(tao.getPowerUnitNumber().substring(0, 5));//前闭后开  前5位 不包括[5]
                // 把数据时标放在最后，方便外层处理
                dataDate = dataList.get(0);
                finalDataList.add(DateUtil.format((Date) dataDate, DateUtil.defaultDatePattern_YMD)); //日冻结类数据时标Td_d(数据日期)
                refreshKey = CommonUtils.refreshKey(tao.getTerminalId(), mpedIdStr, dataList.get(0), businessDataitemId, protocolId);

                break;
            // 月电压统计
            case "13#35":
                finalDataList.add(mpedIdStr);
                Date dataTime = (Date) dataList.get(0);
                String dataDates = DateUtil.format(dataTime, DateUtil.defaultDatePattern_YMD);
                finalDataList.add(dataDates);
                String yeasAppend = yearAndMon.format(dataTime);
                for (int i = 1; i < dataList.size(); i++) {
                    Object os = dataList.get(i);
                    if (os instanceof Date) {
                        Date date = (Date) os;
                        String dayAppend = dayAndHours.format(date);
                        String concatDate = yeasAppend + dayAppend;
                        os = parseDate.parse(concatDate);
                    }
                    finalDataList.add(os);
                }
                finalDataList.add(tao.getPowerUnitNumber());//供电单位编号

                finalDataList.add("00");//未分析
                //区分直抄8/预抄9
                if ("auto".equals(sType)) {
                    data_src = "0";
                } else {
                    data_src = "4";
                }
                finalDataList.add(data_src);//预抄
                finalDataList.add(new Date());//入库时间
                finalDataList.add(tao.getPowerUnitNumber().substring(0, 5));//前闭后开  前5位 不包括[5]
                refreshKey = CommonUtils.refreshKey(tao.getTerminalId(), mpedIdStr, dataList.get(0), businessDataitemId, protocolId);

                break;
            case "13#36":
                finalDataList.add(mpedIdStr);
                for (int i = 0; i < 3; i++) {
                    finalDataList.add(dataList.get(i));
                }
                finalDataList.add(tao.getPowerUnitNumber());//供电单位编号

                finalDataList.add("00");//未分析
                //区分直抄8/预抄9
                if ("auto".equals(sType)) {
                    data_src = "0";
                } else {
                    data_src = "4";
                }
                finalDataList.add(data_src);//预抄
                finalDataList.add(new Date());//入库时间
                finalDataList.add(tao.getPowerUnitNumber().substring(0, 5));//前闭后开  前5位 不包括[5]
                refreshKey = CommonUtils.refreshKey(tao.getTerminalId(), mpedIdStr, dataList.get(0), businessDataitemId, protocolId);

                break;
            //日不平衡度越限累计时间
            case "13#28":
                finalDataList.add(mpedIdStr);
                finalDataList.add(DateUtil.format((Date) dataList.get(0), DateUtil.defaultDatePattern_YMD));//dataDate
                for (int i = 1; i < dataList.size(); i++) {
                    //电流不平衡度越限累计时间
                    //电流不平衡最大值
                    //电流不平衡最大值发生时间
                    //电压不平衡最大值
                    //电压不平衡最大值发生时间
                    finalDataList.add(dataList.get(i));
                }
                finalDataList.add(tao.getPowerUnitNumber());//供电单位编号

                finalDataList.add("00");//未分析
                //区分直抄8/预抄9
                if ("auto".equals(sType)) {
                    data_src = "0";
                } else {
                    data_src = "4";
                }
                finalDataList.add(data_src);//预抄
                finalDataList.add(new Date());//入库时间
                finalDataList.add(tao.getPowerUnitNumber().substring(0, 5));//前闭后开  前5位 不包括[5]
                refreshKey = CommonUtils.refreshKey(tao.getTerminalId(), mpedIdStr, dataList.get(0), businessDataitemId, protocolId);
                break;
            //日冻结掉电记录数据
            case "13#246":
                finalDataList.add(mpedIdStr);
                finalDataList.add(DateUtil.format((Date) dataList.get(0), DateUtil.defaultDatePattern_YMD));//dataDate
                finalDataList.add(tao.getTerminalId());//终端id占位
                String e = (dataList.get(1)).toString();
                if (e.contains("E")) {
                    finalDataList.add(null);
                } else {
                    finalDataList.add(e);
                }
                for (int i = 2; i < dataList.size(); i++) {
                    finalDataList.add(dataList.get(i));
                }

                finalDataList.add(tao.getPowerUnitNumber().substring(0, 5));//前闭后开  前5位 不包括[5]
                finalDataList.add(new Date());//入库时间
                refreshKey = CommonUtils.refreshKey(tao.getTerminalId(), mpedIdStr, dataList.get(0), businessDataitemId, protocolId);

                break;
            //电能表购、用电信息
            case "13#210":
                Date data_date = new Date();
                if (dataList.get(0) != null) {
                    try {
                        data_date = (Date) dataList.get(0);
                    } catch (Exception e1) {

                    }
                }
                finalDataList.add(mpedIdStr);//ID:BIGINT,DATA_DATE:STRING
                finalDataList.add(DateUtil.format(data_date, DateUtil.defaultDatePattern_YMD));//dataDate
                finalDataList.add(dataList.get(1));//抄表日期//,COL_TIME:TIMESTAMP,ORG_NO:STRING,
                finalDataList.add(tao.getPowerUnitNumber());//orgNo
                finalDataList.add(dataList.get(5));//剩余电量//REMAIN_ENEGY:DOUBLE,REMAIN_MONEY:DOUBLE,
                finalDataList.add(dataList.get(3));//剩余金额
                finalDataList.add(dataList.get(9));//报警电量ALARM_ENEGY:DOUBLE,FAIL_ENEGY:DOUBLE,
                finalDataList.add(dataList.get(10));//故障电量
                finalDataList.add(dataList.get(7));//累计购电量SUM_ENEGY:DOUBLE,SUM_MONEY:DOUBLE,
                finalDataList.add(dataList.get(4));//累计购电金额
                finalDataList.add(((Double) dataList.get(2)).intValue());//购电次数BUY_NUM:BIGINT,
                finalDataList.add(dataList.get(8));//赊欠门限值OVERDR_LIMIT:DOUBLE,OVERDR_ENEGY:DOUBLE,
                finalDataList.add(dataList.get(6));//透支电量
                // INSERT_TIME:TIMESTAMP,SHARD_NO:STRING
                finalDataList.add(new Date());//入库时间
                finalDataList.add(tao.getPowerUnitNumber().substring(0, 5));//前闭后开  前5位 不包括[5]
                refreshKey = CommonUtils.refreshKey(tao.getTerminalId(), mpedIdStr, data_date, businessDataitemId, protocolId);

                break;
            //日/月总及分相最大需量及发生时间
            case "13#26":
            case "13#34":
                clock = (Date) dataList.get(0);
                finalDataList.add(mpedIdStr);
                finalDataList.add("0");
                finalDataList.add(clock);
                for (int i = 1; i < dataList.size(); i++) {
                    Object d = dataList.get(i);
                    if (d == null && i == 1) {
                        return;
                    }

                    //hch add
                    finalDataList.add(d);
                    //TODO hch 2020-10-13注释，没有业务要求需要处理，保留原始数据，所以注释
//                    if (fn == 34) {
//                        if (d instanceof Date) {
//                            finalDataList.add(LastMonth(d, clock));
//                        } else {
//                            finalDataList.add(d);
//                        }
//                    } else {
//                        finalDataList.add(d);
//                    }
                }
                finalDataList.add(null);
                finalDataList.add(null);
                finalDataList.add(tao.getPowerUnitNumber());//供电单位编号
                finalDataList.add("00");//未分析
                //区分直抄8/预抄9
                if ("auto".equals(sType)) {
                    data_src = "0";
                } else {
                    data_src = "4";
                }
                finalDataList.add(data_src);//预抄
                finalDataList.add(new Date());//入库时间
                finalDataList.add(tao.getPowerUnitNumber().substring(0, 5));//前闭后开  前5位 不包括[5]
                finalDataList.add(DateUtil.format(clock, DateUtil.defaultDatePattern_YMD));
                refreshKey = CommonUtils.refreshKey(tao.getTerminalId(), mpedIdStr, dataList.get(0), businessDataitemId, protocolId);
                break;
            //固定时间点单相电压
            case "13#250":
            case "13#251":
            case "13#252":
                finalDataList.add(mpedIdStr);//测量点标示
                finalDataList.add(DateUtil.format((Date) dataList.get(0), DateUtil.defaultDatePattern_YMD));//数据日期
                finalDataList.add(tao.getPowerUnitNumber());//ORG_NO
                if (fn == 250) {
                    for (int i = 1; i < 5; i++) {
                        finalDataList.add(dataList.get(i));
                    }
                } else if (fn == 251) {
                    int m = 1;
                    int n = 13;
                    for (int j = 0; j < 4; j++) {//A、B、C三相电压电流
                        finalDataList.add(dataList.get(m));
                        finalDataList.add(dataList.get(m + 1));
                        finalDataList.add(dataList.get(m + 2));
                        m = m + 3;
                        finalDataList.add(dataList.get(n));
                        finalDataList.add(dataList.get(n + 1));
                        finalDataList.add(dataList.get(n + 2));
                        finalDataList.add(dataList.get(n + 3));
                        n = n + 4;
                    }
                } else if (fn == 252) {
                    int m = 1;
                    int n = 2;
                    for (int j = 0; j < 4; j++) {//A、B、C三相电压电流
                        finalDataList.add(dataList.get(m));
                        m = m + 2;
                    }
                    for (int u = 0; u < 4; u++) {
                        finalDataList.add(dataList.get(n));
                        n = n + 2;
                    }
                }
                finalDataList.add("00");//未分析
                //区分直抄8/预抄9
                if ("auto".equals(sType)) {
                    data_src = "0";
                } else {
                    data_src = "4";
                }


                finalDataList.add(data_src);//预抄
                finalDataList.add(new Date());//入库时间
                if (ParamConstants.startWith.equals("11")){
                    if (fn != 250 && fn != 251 && fn != 252 && fn != 37 && fn != 36 && fn != 44 && fn != 153 && fn != 154 && fn != 155 && fn != 156) {
                        finalDataList.add(finalDataList.size(), finalDataList.get(finalDataList.size() - 6));
                        finalDataList.remove(finalDataList.size() - 7);
                    }
                }
                finalDataList.add(tao.getPowerUnitNumber().substring(0, 5));//前闭后开  前5位 不包括[5]
                refreshKey = CommonUtils.refreshKey(tao.getTerminalId(), mpedIdStr, dataList.get(0), businessDataitemId, protocolId);

                break;
            //能源表日冻结示值
            case "13#221":

                String read = String.valueOf(dataList.get(4));
                String readMeter = String.valueOf(dataList.get(3));
                // 现在创建 matcher 对象
                Matcher matcher = r.matcher(read);
                Matcher readM = r.matcher(readMeter);
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                String readNum = null;
                String readSum = null;
                while (matcher.find()) {
                    readNum = matcher.group();
                }
                while (readM.find()) {
                    readSum = readM.group();
                }
                List newGas = new ArrayList();
                List suffix = new ArrayList();
                newGas.add(mpedIdStr);//ID:BIGINT:实际为电表编号
                newGas.add(DateUtil.format((Date) dataList.get(0), DateUtil.defaultDatePattern_YMD));//DATA_DATE:DATE:数据日期 Yyyy-mm-dd
                newGas.add(dataList.get(1));//COLL_TIME:DATETIME:终端抄表时间Yyyy-mm-dd hh24:mi:ss
                newGas.add(readNum);//REAL_SUM_FLOW:DECIMAL:表读数
                newGas.add(readSum);//SETTLE_DAY_SUM_FLOW:DECIMAL:

                suffix.add(new Date());//INSERT_TIME:DATETIME:插入或更新时间
                if ("auto".equals(sType)) {
                    data_src = "0";
                } else {
                    data_src = "4";
                }
                suffix.add(data_src);//DATA_SRC:VARCHAR:抄表方式
                suffix.add(tao.getPowerUnitNumber().substring(0, 5));//SHARD_NO:VARCHAR:分库字段
                List newStatus = new ArrayList();
                String st = (String) dataList.get(7);//已经被翻译了
                String ya = "";
                String za = "";
                if ("00000000".equals(st.substring(0, 8))) {
                    if ("0".equals(st.substring(13, 14))) {
                        ya = "0";
                    } else {
                        ya = "1";
                    }
                    if ("0".equals(st.substring(14, 15)) && "0".equals(st.substring(15, 16))) {
                        za = "00";
                    } else if ("0".equals(st.substring(14, 15)) && "1".equals(st.substring(15, 16))) {
                        za = "01";
                    } else if ("0".equals(st.substring(14, 15)) && "0".equals(st.substring(15, 16))) {
                        za = "11";
                    }
                }
                newStatus.addAll(newGas);
                newStatus.set(3, sdf.parse((String) dataList.get(6)));
                newStatus.set(4, za);
                newStatus.add(ya);
                newStatus.add(new Double(String.valueOf(dataList.get(5))).intValue());

                newGas.addAll(suffix);
                newStatus.addAll(suffix);

                newGas.add("241001_GAS");
                newStatus.add("241001_GAS_STATUS");

                finalDataList.add(newGas);
                finalDataList.add(newStatus);
                refreshKey = CommonUtils.refreshKey(tao.getTerminalId(), mpedIdStr, dataList.get(0), businessDataitemId, protocolId);

                break;
            default:
                if (afn == 13) {
                    boolean allNull = CommonUtils.allNull(dataList);
                    if (allNull) {
                        return;
                    }
                    if (fn >= 161 && fn <= 168) {
                        finalDataList.add(1);//从缓存取测量点标识，这里占位
                        if (fn == 161)
                            finalDataList.add("1");//1正向有功
                        else if (fn == 162)
                            finalDataList.add("2");//正向无功
                        else if (fn == 165)
                            finalDataList.add("3");//一象限无功
                        else if (fn == 168)
                            finalDataList.add("4");//四象限无功
                        else if (fn == 163)
                            finalDataList.add("5");//反向有功
                        else if (fn == 164)
                            finalDataList.add("6");//反向无功
                        else if (fn == 166)
                            finalDataList.add("7");//二象限无功
                        else if (fn == 167)
                            finalDataList.add("8");//三象限无功
                        else
                            finalDataList.add("0");//备用

                        int m = (Integer) dataList.remove(2);//费率个数m  被删除后，后面的往前平移1
                        List list = (List) ((List) dataList.get(3)).get(0);//m个 (m1的正向有功电能示值,m2,m3,m4)
                        Object colDate = dataList.get(1);
                        if (null != colDate && colDate instanceof Date) {
                            finalDataList.add(colDate);
                        } else {
                            finalDataList.add(null);
                        }
                        finalDataList.add(dataList.get(2)); //正向有功总电能示值
                        finalDataList.addAll(list);//4个值的list，一次性添加
                        for (int i = m; i < 14; i++) {//补充14-m个正向有功电能示值
                            finalDataList.add(null);
                        }
                        finalDataList.add(1, mpedIdStr);//首元素id

                        finalDataList.add(tao.getPowerUnitNumber());//供电单位编号

                        finalDataList.add("00");//未分析
                        //区分直抄8/预抄9
                        if ("auto".equals(sType)) {
                            data_src = "0";
                        } else {
                            data_src = "4";
                        }
                        finalDataList.add(data_src);//预抄
                        finalDataList.add(new Date());//入库时间
                        finalDataList.add(tao.getPowerUnitNumber().substring(0, 5));//前闭后开  前5位 不包括[5]
                        // 把数据时标放在最后，方便外层处理
                        dataDate = dataList.get(0);
                        Date readDate = (Date) dataDate;
                        finalDataList.add(DateUtil.format(readDate, DateUtil.defaultDatePattern_YMD)); //日冻结类数据时标Td_d(数据日期)
                        if (DateFilter.isLateWeek(readDate, -45)) {
                            return;
                        }
                        finalDataList.set(0, mpedIdStr + "_" + finalDataList.get(finalDataList.size() - 1));
                        refreshKey = CommonUtils.refreshKey(tao.getTerminalId(), mpedIdStr, dataList.get(0), businessDataitemId, protocolId);

                    } else if (fn >= 5 && fn <= 8) {
                        finalDataList = new ArrayList();
                        finalDataList.add(1);
                        if (fn == 5)
                            finalDataList.add("1");//1正向有功
                        else if (fn == 6)
                            finalDataList.add("2");//正向无功
                        else if (fn == 7)
                            finalDataList.add("5");//反向有功
                        else if (fn == 8)
                            finalDataList.add("6");//反向无功
                        // 把数据时标放在最后，方便外层处理
                        dataDate = dataList.get(0);
                        int m = (Integer) dataList.remove(1);//费率个数m  被删除后，后面的往前平移1
                        List list = (List) ((List) dataList.get(2)).get(0);//m个 (m1的正向有功电能示值,m2,m3,m4)
                        finalDataList.add(dataDate);//这个时间是个填充值，后面要删掉的，只是站位
                        finalDataList.add(dataList.get(1)); //日正向有功总电能示值
                        finalDataList.addAll(list);
                        for (int i = 0; i < 4 - list.size(); i++) {
                            finalDataList.add(null);
                        }
                        for (int j = m; j < 14; j++) {//补充14-m个正向有功电能示值
                            finalDataList.add(null);
                        }
                        finalDataList.set(0, mpedIdStr);

                        if (ParamConstants.startWith.equals("11")) {
                            finalDataList.add(tao.getPowerUnitNumber());//供电单位编号
                        } else {
                            if (fn == 250 || fn == 251 || fn == 252) {
                                finalDataList.set(2, tao.getPowerUnitNumber());
                            } else {
                                finalDataList.add(tao.getPowerUnitNumber());
                            }
                        }

                        finalDataList.add("00");//未分析
                        //区分直抄8/预抄9
                        if ("auto".equals(sType)) {
                            data_src = "0";
                        } else {
                            data_src = "4";
                        }
                        finalDataList.add(data_src);//预抄
                        finalDataList.add(new Date());//入库时间
                        finalDataList.add(tao.getPowerUnitNumber().substring(0, 5));//前闭后开  前5位 不包括[5]
                        finalDataList.add(DateUtil.format((Date) dataDate, DateUtil.defaultDatePattern_YMD)); //日冻结类数据时标Td_d(数据日期)
                        refreshKey = CommonUtils.refreshKey(tao.getTerminalId(), mpedIdStr, dataList.get(0), businessDataitemId, protocolId);

                    } else if ((fn >= 185 && fn <= 188) || (fn >= 193 && fn <= 196)) {//需量+时间
                        finalDataList = new ArrayList();
                        finalDataList.add(mpedIdStr);
                        if (fn == 185 || fn == 193) {
                            finalDataList.add("1");
                        } else if (fn == 186 || fn == 194) {
                            finalDataList.add("2");
                        } else if (fn == 187 || fn == 195) {
                            finalDataList.add("5");
                        } else if (fn == 188 || fn == 196) {
                            finalDataList.add("6");
                        }

                        finalDataList.add(dataList.get(1));//
                        Object obj_value = dataList.get(3);
                        if (obj_value == null) {
                            return;
                        }
                        finalDataList.add(obj_value);
                        Object obj4 = dataList.get(4);
                        calDate = Calendar.getInstance();
                        calDataDate = Calendar.getInstance();
                        calDataDate.setTime((Date) dataList.get(0));
                        if (obj4 != null) {
                            calDate.setTime((Date) obj4);
                            calDate.set(Calendar.YEAR, calDataDate.get(Calendar.YEAR));
                            obj4 = calDate.getTime();
                        }
//                        if (!(obj4 instanceof Date)) {
//                            return;
//                        }
                        finalDataList.add(obj4);
                        if (ParamConstants.startWith.equals("11")) {
                            List dt = (List) ((List) dataList.get(5)).get(0);
                            for (int i = 0; i < dt.size(); i++) {
                                Object o = dt.get(i);
                                if (o == null) {
                                    finalDataList.add(null);
                                } else {
                                    if (i % 2 != 0) {
                                        Calendar cel = Calendar.getInstance();
                                        Date time = (Date) o;
                                        cel.setTime(time);
                                        cel.set(Calendar.YEAR, calDataDate.get(Calendar.YEAR));
                                        finalDataList.add(cel.getTime());
                                    } else {
                                        finalDataList.add(o);
                                    }
                                }
                            }
                        }
                        finalDataList.add(tao.getPowerUnitNumber());//供电单位编号

                        finalDataList.add("00");//未分析
                        //区分直抄8/预抄9
                        if ("auto".equals(sType)) {
                            data_src = "0";
                        } else {
                            data_src = "4";
                        }
                        finalDataList.add(data_src);//预抄
                        finalDataList.add(new Date());//入库时间
                        finalDataList.add(tao.getPowerUnitNumber().substring(0, 5));//前闭后开  前5位 不包括[5]
                        dataDate = dataList.get(0);
                        finalDataList.add(DateUtil.format((Date) dataDate, DateUtil.defaultDatePattern_YMD)); //日冻结类数据时标Td_d(数据日期)
                        refreshKey = CommonUtils.refreshKey(tao.getTerminalId(), mpedIdStr, dataList.get(0), businessDataitemId, protocolId);

                    } else if (fn >= 177 && fn <= 184) {//********************以下是月冻结判断******************************
                        finalDataList.add(1);//从缓存取测量点标识，这里占位
                        if (fn == 177)
                            finalDataList.add("1");//1正向有功
                        else if (fn == 178)
                            finalDataList.add("2");//正向无功
                        else if (fn == 181)
                            finalDataList.add("3");//一象限无功
                        else if (fn == 184)
                            finalDataList.add("4");//四象限无功
                        else if (fn == 179)
                            finalDataList.add("5");//反向有功
                        else if (fn == 180)
                            finalDataList.add("6");//反向无功
                        else if (fn == 182)
                            finalDataList.add("7");//二象限无功
                        else if (fn == 183)
                            finalDataList.add("8");//三象限无功
                        else
                            finalDataList.add("0");//备用

                        int m = (Integer) dataList.remove(2);//费率个数m  被删除后，后面的往前平移1
                        List list = (List) ((List) dataList.get(3)).get(0);//m个 (m1的正向有功电能示值,m2,m3,m4)
                        Object colDate = dataList.get(1);
                        if (null != colDate && colDate instanceof Date) {
                            finalDataList.add(colDate);
                        } else {
                            finalDataList.add(null);
                        }
                        finalDataList.add(dataList.get(2)); //正向有功总电能示值
                        finalDataList.addAll(list);//4个值的list，一次性添加
                        for (int i = m; i < 14; i++) {//补充14-m个正向有功电能示值
                            finalDataList.add(null);
                        }
                        finalDataList.add(1, mpedIdStr);//首元素id

                        finalDataList.add(tao.getPowerUnitNumber());//供电单位编号

                        finalDataList.add("00");//未分析
                        //区分直抄8/预抄9
                        if ("auto".equals(sType)) {
                            data_src = "0";
                        } else {
                            data_src = "4";
                        }
                        finalDataList.add(data_src);//预抄
                        finalDataList.add(new Date());//入库时间
                        finalDataList.add(tao.getPowerUnitNumber().substring(0, 5));//前闭后开  前5位 不包括[5]
                        // 把数据时标放在最后，方便外层处理
                        dataDate = dataList.get(0);
                        finalDataList.add(DateUtil.format((Date) dataDate, DateUtil.defaultDatePattern_YMD)); //日冻结类数据时标Td_d(数据日期)
                        finalDataList.set(0, mpedIdStr + "_" + finalDataList.get(finalDataList.size() - 1));
                        refreshKey = CommonUtils.refreshKey(tao.getTerminalId(), mpedIdStr, dataList.get(0), businessDataitemId, protocolId);

                    } else if (fn >= 21 && fn <= 24) {//yue
                        finalDataList.add(1);//从缓存取测量点标识，这里占位
                        if (fn == 21)
                            finalDataList.add("1");//1正向有功
                        else if (fn == 22)
                            finalDataList.add("2");//正向无功
                        else if (fn == 23)
                            finalDataList.add("5");//一象限无功
                        else if (fn == 24)
                            finalDataList.add("6");//四象限无功
                        else
                            finalDataList.add("0");//备用

                        int m = (Integer) dataList.remove(1);//费率个数m  被删除后，后面的往前平移1
                        List list = (List) ((List) dataList.get(2)).get(0);//m个 (m1的正向有功电能示值,m2,m3,m4)
                        finalDataList.add(null);//这个时间是个填充值，后面要删掉的，只是站位
                        finalDataList.add(dataList.get(1)); //日正向有功总电能示值
                        if (list.size() < 4) {
                            finalDataList.addAll(list);
                            for (int i = 0; i < 4 - list.size(); i++) {
                                finalDataList.add(null);
                            }
                        }
                        for (int i = m; i < 14; i++) {//补充14-m个正向有功电能示值
                            finalDataList.add(null);
                        }
                        finalDataList.set(0, mpedIdStr);

                        finalDataList.add(tao.getPowerUnitNumber());//供电单位编号

                        finalDataList.add("00");//未分析
                        //区分直抄8/预抄9
                        if ("auto".equals(sType)) {
                            data_src = "0";
                        } else {
                            data_src = "4";
                        }
                        finalDataList.add(data_src);//预抄
                        finalDataList.add(new Date());//入库时间
                        finalDataList.add(tao.getPowerUnitNumber().substring(0, 5));//前闭后开  前5位 不包括[5]
                        // 把数据时标放在最后，方便外层处理
                        dataDate = dataList.get(0);
                        finalDataList.add(DateUtil.format((Date) dataDate, DateUtil.defaultDatePattern_YMD)); //日冻结类数据时标Td_d(数据日期)
                        refreshKey = CommonUtils.refreshKey(tao.getTerminalId(), mpedIdStr, dataList.get(0), businessDataitemId, protocolId);

                    } else if (fn >= 153 && fn <= 156) {//分相示值
                        String types = "";
                        boolean allNulls = true;
                        finalDataList.add(mpedIdStr);
                        if (fn == 153) {//日冻结分相正相有功电能示值
                            types = "1";
                        } else if (fn == 154) {
                            types = "2";
                        } else if (fn == 155) {
                            types = "5";
                        } else if (fn == 156) {
                            types = "6";
                        }
                        finalDataList.add(types);
                        finalDataList.add(DateUtil.format((Date) dataList.get(0), DateUtil.defaultDatePattern_YMD));
                        finalDataList.add(dataList.get(1));
                        for (int i = 2; i < dataList.size(); i++) {
                            if (dataList.get(i) instanceof Double) {
                                allNulls = false;
                                finalDataList.add(dataList.get(i));
                            } else {
                                finalDataList.add(null);
                            }
                        }
                        finalDataList.add(tao.getPowerUnitNumber());//供电单位编号

                        finalDataList.add("00");//未分析
                        //区分直抄8/预抄9
                        if ("auto".equals(sType)) {
                            data_src = "0";
                        } else {
                            data_src = "4";
                        }
                        finalDataList.add(data_src);//预抄
                        finalDataList.add(new Date());//入库时间
                        finalDataList.add(tao.getPowerUnitNumber().substring(0, 5));//前闭后开  前5位 不包括[5]
                        refreshKey = CommonUtils.refreshKey(tao.getTerminalId(), mpedIdStr, dataList.get(0), businessDataitemId, protocolId);

                        if (allNulls) {
                            return;
                        }
                    }
                }
        }


        if (finalDataList.size() < 1) {
            return;
        }
        if (!"auto".equals(sType)) {
            refreshKey = null;
        }
        if (finalDataList.get(0) instanceof List) {
            for (int i = 0, j = finalDataList.size(); i < j; i++) {
                List df = (List) finalDataList.get(i);
                if (refreshKey != null && i != 0) {
                    refreshKey = null;
                }
                if ("13#221".equals(afnAndfn)) {
                    businessDataitemId = (String) df.remove(df.size() - 1);
                }
                CommonUtils.putToDataHub(businessDataitemId, terminalId, df, refreshKey, listDataObj);
            }
        } else {
            CommonUtils.putToDataHub(businessDataitemId, terminalId, finalDataList, refreshKey, listDataObj);
        }

//        if ((fn >= 193 && fn <= 196)) {
//            List mon2dayList = finalDataList;
//            int dateIndex = mon2dayList.size() - 1;
//            String dataDate = mon2dayList.get(dateIndex).toString();
//            String lastDataDate = lastDayByMonth(dataDate);
//            mon2dayList.set(dateIndex, lastDataDate);
//            refreshKey[2]=clearDate(dataDate);
//            CommonUtils.putToDataHub("205003", terminalId, mon2dayList, refreshKey, listDataObj);
//        }

    }

    private static Date clearDate(String dataDate) throws ParseException {
        Date data = DateUtil.parse(dataDate);
        Calendar first = Calendar.getInstance();
        first.setTime(data);
        first.set(Calendar.DAY_OF_MONTH, -1);
        return DateUtil.parse(DateUtil.format(first.getTime(), "yyyyMMdd"));
    }

    private static String lastDayByMonth(String dataDate) throws Exception {
        Date data = DateUtil.parse(dataDate);
        Calendar first = Calendar.getInstance();
        first.setTime(data);
        Calendar last = Calendar.getInstance();
        last.set(Calendar.YEAR, first.get(Calendar.YEAR));
        last.set(Calendar.MONTH, first.get(Calendar.MONTH));
        last.set(Calendar.DAY_OF_MONTH, 1);
        return DateUtil.format(last.getTime(), DateUtil.defaultDatePattern_YMD);
    }
}
