package com.tl.hades.persist;

import com.ls.athena.dataprocess.sg3761.beans.DataItemObject;
import com.ls.athena.dataprocess.sg3761.beans.TerminalDataObject;
import com.ls.athena.framew.terminal.archivesmanager.Protocol3761ArchivesObject;
import com.ls.athena.framew.terminal.archivesmanager.ProtocolArchives;
import com.ls.athena.framew.terminal.archivesmanager.TerminalArchives;
import com.ls.athena.framew.terminal.archivesmanager.TerminalArchivesObject;
import com.ls.pf.base.utils.tools.StringUtils;
import com.tl.dataprocess.param.ParamConstants;
import com.tl.easb.utils.DateUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;


/**
 * @author Dongwei-Chen
 * @Date 2020/5/21 19:24
 * @Description 实时数据组装
 */
public class CurrentDataPackage {

    private final static Logger logger = LoggerFactory.getLogger(CurrentDataPackage.class);

    private static String[] items = {"201015", "201016", "201017", "201018"};
    private static List<String> itemList = Arrays.asList(items);

    public static void getDataList(TerminalDataObject terminalDataObject, int protocolId, List<DataObject> listDataObj, DataItemObject data, String sType) throws Exception {
        List dataList = data.getList();
        if (dataList.size() == 0) {
            return;
        }
        int afn = terminalDataObject.getAFN();
        int fn = data.getFn();
        int pn = data.getPn();
        Date clock = terminalDataObject.getUpTime();
        String areaCode = terminalDataObject.getAreaCode();
        int terminalAddr = terminalDataObject.getTerminalAddr();
        logger.info("===MAKE " + afn + "_" + fn + " DATA===");

        Protocol3761ArchivesObject protocol3761ArchivesObject = CommonUtils.getProArchive(protocolId, afn, fn);

        String businessDataitemId = ProtocolArchives.getInstance().getProtocolArchivesObject(protocol3761ArchivesObject).getBusiDataItemId();

        String afnAndfn = afn + "#" + fn;
        TerminalArchivesObject tao = TerminalArchives.getInstance().getTmnlAndMpedIdArchives(areaCode, String.valueOf(terminalAddr), "P" + pn);
        if (tao == null) {
            return;
        }
        Object[] refreshKey = null;
        String data_src;
        String terminalId = tao.getTerminalId();
        String mpedIdStr = tao.getID();
        List finalDataList = new ArrayList();
        List dataFinalList = new ArrayList();
        switch (afnAndfn) {
            case "10#7":
                //终端ip端口召测
                finalDataList.add(tao.getTerminalId());//TERMINAL_ID:BIGINT:终端ID
                String ip = (String) dataList.get(0);
                String[] ipSplit = ip.split("\\.");
                for (int i = 0; i < 4; i++) {
                    finalDataList.add(ipSplit[i]);//TMNL_IP1:INT:终端IP地址1段
                }
                String mask = (String) dataList.get(1);
                String[] maskSplit = mask.split("\\.");
                for (int i = 0; i < 4; i++) {
                    finalDataList.add(maskSplit[i]);//SUBNET_IP1:INT:子网掩码地址1段
                }
                String netWork = (String) dataList.get(2);
                String[] netWorkSplit = netWork.split("\\.");
                for (int i = 0; i < 4; i++) {
                    finalDataList.add(netWorkSplit[i]);//GATEWAY_IP1:INT:网关地址1段
                }
                finalDataList.add(dataList.get(3));//PROXY_TYPE:TINYINT:代理类型：数值范围0～3，依次表示：不使用代理、httpconnect代理、socks4代理、socks5代理。
                String proc = (String) dataList.get(4);
                String[] procSplit = proc.split("\\.");
                for (int i = 0; i < 4; i++) {
                    finalDataList.add(procSplit[i]);//PROXY_IP1:INT:代理服务器地址1段
                }
                finalDataList.add(dataList.get(5));//PROXY_PORT:INT:代理服务器端口
                finalDataList.add(dataList.get(6));//PROXY_MODE:TINYINT:代理服务器连接方式：数值范围0～1，依次表示：无需验证、需要用户名/密码。
                String user = (String) dataList.get(7);
                finalDataList.add(user.length());//USER_LEN:TINYINT:用户名长度
                finalDataList.add(user);//USER_NO:VARCHAR:用户名
                String password = (String) dataList.get(8);

                finalDataList.add(password.length());//PASSWORD_LEN:TINYINT:密码长度
                finalDataList.add(password);//PASSWORD:VARCHAR:密码
                finalDataList.add(dataList.get(9));//TMNL_PORT:INT:终端侦听端口
                finalDataList.add(null);//OPERATOR_NO:VARCHAR:操作人员
                finalDataList.add(null);//OPERATE_TIME:DATETIME:操作时间
                finalDataList.add(tao.getPowerUnitNumber().substring(0, 5));//SHARD_NO:VARCHAR:分库字段
                finalDataList.add(new Date());//INSERT_TIME:DATETIME:插入或更新时间
                refreshKey = CommonUtils.refreshKey(tao.getTerminalId(), tao.getTerminalId(), "00000000000000", businessDataitemId, protocolId);
                break;
            case "10#15":
                if (sType.equals("front")) refreshKey = null;
                if (dataList.size() < 2) {
                    return;
                }
                List alldtList = (List) dataList.get(1);
                if (alldtList.size() == 0) {
                    return;
                }
                int nodeCount = alldtList.size();
                for (int i = 0; i < nodeCount; i++) {
                    List listValue = new ArrayList();
                    List dtList = (List) alldtList.get(i);
                    String commaddr = dtList.get(0).toString();
                    tao = TerminalArchives.getInstance().getTmnlAndMpedIdArchives(areaCode, String.valueOf(terminalAddr), "COMMADDR" + commaddr);
                    String mpedId = tao.getID();
                    if (terminalId == null || mpedId == null) {
                        continue;
                    }
                    listValue.add(terminalId);//TERMINAL_ID:BIGINT:终端设备标识
                    listValue.add(i);//NODE_IDEX:TINYINT:节点序号,0:主节点，1 - n:从节点
                    listValue.add(areaCode);//AREA_CODE:VARCHAR:区域码
                    listValue.add(terminalAddr);//TERMINAL_ADDR:VARCHAR:终端地址码
                    listValue.add(mpedId);//MPED_ID:BIGINT:当节点序号为0时为终端id，当不为0时需要根据终端id和节点地址获取mped_id
                    listValue.add(DateUtil.format(new Date(), DateUtil.defaultDatePattern_YMD));//DATA_DATE:DATE:数据日期
                    listValue.add(commaddr);//NODE_ADDR:VARCHAR:节点地址,当为从节点时节点地址为电表地址
                    listValue.add(dtList.get(1));//SOFTWARE_VERSION_NO:VARCHAR:软件版本号
                    String rq = "20" + dtList.get(4) + "-" + dtList.get(3) + "-" + dtList.get(2);
                    Date softDate = null;
                    try {
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                        softDate = sdf.parse(rq);
                    } catch (Exception e) {
                        continue;
                    }
                    listValue.add(softDate);//SOFTWARE_VERSION_DATE:DATE:软件版本日期

                    listValue.add(dtList.get(5));//MODULE_VENDOR_CODE:VARCHAR:模块厂商代码
                    listValue.add(dtList.get(6));//CHIP_CODE:VARCHAR:芯片代码
                    listValue.add(tao.getPowerUnitNumber().substring(0, 5));//SHARD_NO:VARCHAR:分库字段
                    listValue.add(new Date());//INSERT_TIME:DATETIME:插入或更新时间

                    finalDataList.add(listValue);
                }
                break;
            case "10#112":
                List ddlist = (List) dataList.get(1);
                if (ddlist == null) {
                    return;
                }
                Date endD = new Date();
                try {
                    SimpleDateFormat sdf = new SimpleDateFormat("MMM d,yyyy K:m:s a", Locale.ENGLISH);
                    if (dataList.get(0).toString().contains(",")) {
                        endD = sdf.parse(dataList.get(0).toString());
                    } else {
                        endD = (Date) dataList.get(0);
                    }
                } catch (Exception e) {
                    logger.error("the data is error!");
                }

                for (int m = 0; m < ddlist.size(); m++) {
                    List dlist = (List) ddlist.get(m);
                    List listValue = new ArrayList();
                    listValue.add(tao.getTerminalId());//TERMINAL_ID:BIGINT:终端ID
                    listValue.add(ddlist.size());//MATER_NUM:TINYINT:表计数量
                    listValue.add(dlist.get(0));//SEQ_NUM:VARCHAR:上报序号
                    listValue.add(dlist.get(2));//TMNL_BAR_CODE:VARCHAR:所属采集器资产号
                    listValue.add(dlist.get(1));//COMM_ADDR:VARCHAR:电表地址
                    listValue.add(dlist.get(3));//COMM_PROTOCOL:VARCHAR:电表规约
                    listValue.add(endD);//REPORT_TIME:DATETIME:上报时间
                    listValue.add(new Date());//INSERT_TIME:DATETIME:插入或更新时间
                    listValue.add(tao.getPowerUnitNumber().substring(0, 5));//SHARD_NO:VARCHAR:分库字段
                    listValue.add(DateUtil.format(endD, DateUtil.defaultDatePattern_YMD));
                    finalDataList.add(listValue);
                }
                refreshKey = CommonUtils.refreshKey(tao.getTerminalId(), tao.getTerminalId(), "00000000000000", businessDataitemId, protocolId);
                break;
            //终端电能表/交流采样装置配置参数
            case "10#10":
                if (null == terminalId || "".equals(terminalId)) {
                    logger.error("无法从缓存获取正确的档案信息:" + areaCode + "_" + terminalId + "_TMNLID");
                    return;
                }

                for (Object o : dataList) {
                    for (Object oo : (List) o) {
                        List listValue = new ArrayList();
                        for (Object ooo : (List) oo) {
                            if (ooo instanceof byte[]) {
                                listValue.add(StringUtils.encodeHex((byte[]) ooo));
                            } else {
                                listValue.add(ooo);
                            }
                        }
                        Map<Integer, Integer> protocolmap = new HashMap<>();
                        protocolmap.put(1, 2);
                        protocolmap.put(30, 3);
                        protocolmap.put(2, 5);
                        protocolmap.put(31, 6);
                        protocolmap.put(32, 32);
                        protocolmap.put(100, 100);
                        protocolmap.put(3, 8);
                        Map<Integer, String> userflagmap = new HashMap<Integer, String>();
                        userflagmap.put(0, "01");
                        userflagmap.put(1, "02");
                        userflagmap.put(2, "03");
                        userflagmap.put(3, "04");
                        userflagmap.put(4, "05");
                        userflagmap.put(5, "06");
                        listValue.set(2, "0" + listValue.get(2));
                        listValue.set(4, protocolmap.get(listValue.get(4)));
                        listValue.set(8, (int) (listValue.get(8)) + 4);
                        listValue.set(9, (int) (listValue.get(9)) + 1);
                        if (tao.getPowerUnitNumber().startsWith("4")) {//4开头的代表河南
                            listValue.set(11, userflagmap.get(listValue.get(11)));
                        } else {
                            int fFlag = Integer.valueOf(listValue.get(11).toString());
                            String endFlag = "0" + String.valueOf(fFlag);
                            listValue.set(11, endFlag);
                        }
                        listValue.add(new Date());
                        listValue.add(tao.getPowerUnitNumber().substring(0, 5));
                        listValue.add(0, terminalId);

                        finalDataList.add(listValue);
                    }
                }
                refreshKey = CommonUtils.refreshKey(terminalId, terminalId, "00000000000000", businessDataitemId, protocolId);
                break;
            //重点用户
            case "10#35":
                if (dataList.size() == 0) {
                    return;
                }

                List dtList = (List) dataList.get(0);
                if (dtList.size() == 0) {
                    return;
                }
                for (int i = 0, len = dtList.size();
                     i < len; i++) {
                    dataFinalList = new ArrayList();

                    Integer mpedIndex = (Integer) ((List) dtList.get(i)).get(0);
                    TerminalArchivesObject terminalArchivesObject = TerminalArchives.getInstance().getTmnlAndMpedIdArchives(areaCode, String.valueOf(terminalAddr), "P" + mpedIndex);
                    mpedIdStr = terminalArchivesObject.getID();
                    terminalId = terminalArchivesObject.getTerminalId();
                    if (mpedIdStr == null || terminalId == null) {
                        continue;
                    }
                    dataFinalList.add(new BigDecimal(mpedIdStr));
                    dataFinalList.add(terminalId);
                    dataFinalList.add(mpedIndex);
                    dataFinalList.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));
                    dataFinalList.add(new Date());
                    finalDataList.add(dataFinalList);
                    refreshKey = CommonUtils.refreshKey(tao.getTerminalId(), mpedIdStr, "00000000000000", businessDataitemId, protocolId);
                }
            case "10#5":
                //终端上行通信消息认证
                finalDataList.add(new BigDecimal(terminalId));//TERMINAL_ID:VARCHAR:终端ID
                finalDataList.add(dataList.get(0));//MESSAGE_AUTHENTICATION_NO:INT:消息认证方案号：用于表示由系统约定的各种消息认证方案，取值范围0～255，其中：0表示不认证，255表示专用硬件认证方案，1～254用于表示各种软件认证方案。
                finalDataList.add(dataList.get(1));//MESSAGE_AUTHENTICATION_PARAM:VARCHAR:消息认证方案参数
                finalDataList.add(tao.getPowerUnitNumber().substring(0, 5));//SHARD_NO:VARCHAR:分库字段
                finalDataList.add(new Date());//INSERT_TIME:DATETIME:插入或更新时间
                refreshKey = CommonUtils.refreshKey(terminalId, terminalId, "00000000000000", businessDataitemId, protocolId);
                break;
            case "10#25":
                if(ParamConstants.startWith.equals("11"))
                    tao = TerminalArchives.getInstance().getTmnlAndMpedIdArchives(areaCode, String.valueOf(terminalAddr), "P" + pn);
                if (tao == null) {
                    logger.error("无法从缓存获取正确的档案信息:" + areaCode + "_" + terminalAddr);
                    return;
                }
                String mpedIdStrs = tao.getID();
                if (mpedIdStrs == null) {
                    logger.error("无法从缓存获取正确的档案信息:" + areaCode + "_" + terminalAddr);
                    return;
                }
                refreshKey[1] = mpedIdStr;
                refreshKey = CommonUtils.refreshKey(terminalId, terminalId, "00000000000000", businessDataitemId, protocolId);
                break;
            //终端时钟
            case "12#2":
                finalDataList.add(new BigDecimal(terminalId));
                finalDataList.add(areaCode);
                finalDataList.add(terminalAddr);
                finalDataList.add(dataList.get(0));
                finalDataList.add(clock);//时间（主站）
                Date date = (Date) dataList.get(0);
                Long cha = Math.abs(date.getTime() - System.currentTimeMillis());
                finalDataList.add(new java.text.DecimalFormat("0.00").format(Double.parseDouble(cha.toString()) / (1000 * 60)));//时钟超差值
                finalDataList.add(0);//连续对时次数
                finalDataList.add(new Date());
                finalDataList.add(tao.getPowerUnitNumber().substring(0, 5));
                refreshKey = CommonUtils.refreshKey(terminalId, terminalId, "00000000000000", businessDataitemId, protocolId);
                break;
            //表箱及电表总数
            case "12#59":
                List datatoredis = new ArrayList<Object>();

                finalDataList.add(mpedIdStr);
                finalDataList.add(DateUtil.format(new Date(), DateUtil.defaultDatePattern_YMD));
                for (Object ob : dataList) {
                    finalDataList.add(ob);
                }
                datatoredis.addAll(finalDataList);
                finalDataList.add(tao.getPowerUnitNumber());//供电单位编号
                //区分直抄8/预抄9
                if ("auto".equals(sType)) {
                    data_src = "10";
                } else {
                    data_src = "14";
                }
                finalDataList.add(data_src);//预抄
                finalDataList.add(new Date());//入库时间3531778244
                finalDataList.add(tao.getPowerUnitNumber().substring(0, 5));//前闭后开  前5位 不包括[5]
                refreshKey = CommonUtils.refreshKey(terminalId, mpedIdStr, "00000000000000", businessDataitemId, protocolId);
                if (ParamConstants.startWith.equals("11")) {
                    //分支箱写缓存
                    TerminalArchives.getInstance().putfzx(areaCode, terminalAddr, datatoredis);
                }
                break;
            //表箱当前数据
            case "12#60":
                List datal = (List) dataList.get(0);
                for (int j = 0; j < datal.size(); j++) {
                    List dataListFinal = new ArrayList<>();
                    List datas = (List) datal.get(j);
                    dataListFinal.add(DateUtil.format(new Date(), DateUtil.defaultDatePattern_YMD));
                    dataListFinal.add(datas.get(0));
                    dataListFinal.add(datas.get(1));
                    dataListFinal.add(terminalAddr);
                    dataListFinal.add(areaCode);
                    dataListFinal.add(null);
                    dataListFinal.add(null);
                    for (int i = 4; i < 17; i++) {
                        dataListFinal.add(datas.get(i));
                    }
                    dataListFinal.add(datas.get(19));
                    dataListFinal.add(datas.get(20));
                    dataListFinal.add(datas.get(21));
                    dataListFinal.add(tao.getPowerUnitNumber());//供电单位编号
                    //区分直抄8/预抄9
                    if ("auto".equals(sType)) {
                        data_src = "10";
                    } else {
                        data_src = "14";
                    }
                    dataListFinal.add(data_src);//预抄
                    dataListFinal.add(new Date());//入库时间3531778244
                    dataListFinal.add(new Date());//入库时间3531778244
                    dataListFinal.add(tao.getPowerUnitNumber().substring(0, 5));//前闭后开  前5位 不包括[5]
                    finalDataList.add(dataFinalList);
                    refreshKey = CommonUtils.refreshKey(terminalId, mpedIdStr, "00000000000000", businessDataitemId, protocolId);
                    break;
                }
                //表箱内电表信息
            case "12#61":
                datal = (List) dataList.get(0);
                for (int j = 0; j < datal.size(); j++) {
                    List dataListFinal = new ArrayList<>();
                    List datas = (List) datal.get(j);
                    String maddr = (String) datas.get(1);
                    TerminalArchivesObject terminalArchivesObject;
                    if(ParamConstants.startWith.equals("11")){
                        terminalArchivesObject = TerminalArchives.getInstance().getTmnlAndMMidArchives(areaCode, String.valueOf(terminalAddr), "COMMADDR" + maddr, "MI" + maddr);
                    }else{
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
                    dataListFinal.add(mpedIdStr);
                    dataListFinal.add(DateUtil.format(new Date(), DateUtil.defaultDatePattern_YMD));
                    for (int i = 0; i < 4; i++) {
                        dataListFinal.add(datas.get(i));
                    }
                    dataListFinal.add(terminalAddr);
                    dataListFinal.add(areaCode);
                    dataListFinal.add(datas.get(6));
                    for (int i = 9; i < datas.size() - 2; i++) {
                        dataListFinal.add(datas.get(i));
                    }
                    dataListFinal.add(null);
                    dataListFinal.add(null);
                    dataListFinal.add(terminalArchivesObject.getPowerUnitNumber());//供电单位编号
                    //区分直抄8/预抄9
                    if ("auto".equals(sType)) {
                        data_src = "10";
                    } else {
                        data_src = "14";
                    }
                    dataListFinal.add(data_src);//预抄
                    dataListFinal.add(new Date());//入库时间3531778244
                    dataListFinal.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));//前闭后开  前5位 不包括[5]
                    finalDataList.add(dataListFinal);
                    refreshKey = CommonUtils.refreshKey(tao.getTerminalId(), mpedIdStr, "00000000000000", businessDataitemId, protocolId);
                }
                break;
            //巡检仪：电流回路状态
            case "12#50":
                if (mpedIdStr == null || terminalId == null) {
                    logger.error("无法从缓存获取正确的档案信息:" + areaCode + "_" + terminalAddr);
                    return;
                }

                finalDataList.add(BigDecimal.valueOf(Long.valueOf(mpedIdStr)));
                if (dataList.size() != 0) {
                    for (Object o : dataList) {
                        finalDataList.add(o);
                    }
                }
                if (finalDataList.size() != 5) {
                    return;
                }
                finalDataList.add(DateUtil.format(new Date(), DateUtil.defaultDatePattern_YMD));
                finalDataList.add(tao.getPowerUnitNumber().substring(0, 5));
                finalDataList.add(new Date());
                refreshKey = CommonUtils.refreshKey(tao.getTerminalId(), mpedIdStr, "00000000000000", businessDataitemId, protocolId);
                break;
            //巡检仪：CT状态
            case "12#51":
                if (mpedIdStr == null || terminalId == null) {
                    logger.error("无法从缓存获取正确的档案信息:" + areaCode + "_" + terminalAddr);
                    return;
                }

                finalDataList.add(new BigDecimal(mpedIdStr));
                finalDataList.add(DateUtil.format(new Date(), DateUtil.defaultDatePattern_YMD));
                if (dataList.size() != 0) {
                    int j = 13;
                    for (int i = 0; i < dataList.size(); i++) {
                        Object o = dataList.get(i);
                        //转换成bigint
                        if (i == j && j <= 37) {
                            Double s = (Double) o;
                            finalDataList.add(BigDecimal.valueOf(Long.valueOf(s.intValue())));
                            j += 3;
                        } else {
                            finalDataList.add(o);
                        }
                    }
                }
                finalDataList.add(tao.getPowerUnitNumber().substring(0, 5));
                finalDataList.add(new Date());
                refreshKey = CommonUtils.refreshKey(tao.getTerminalId(), mpedIdStr, "00000000000000", businessDataitemId, protocolId);
                break;
            //全网感知
            case "12#252":
                if (terminalId == null) {
                    logger.error("无法从缓存获取正确的档案信息:" + areaCode + "_" + terminalAddr);
                    return;
                }
                Date percDate = (Date) dataList.get(0);
                if (percDate == null) {
                    return;
                }
                Calendar calendar = getCalendar();
                calendar.setTime(percDate);
                int min = calendar.get(Calendar.HOUR_OF_DAY) * 60 + calendar.get(Calendar.MINUTE);//获取给定数据时标在当天所在的分钟数2
                int a = min / 5;
                SimpleDateFormat sdf = new SimpleDateFormat("yyMMddHHmm");
                finalDataList.add(terminalId);
                finalDataList.add(DateUtil.format(percDate, DateUtil.defaultDatePattern_YMD));
                finalDataList.add(tao.getPowerUnitNumber());
                finalDataList.add(sdf.format(percDate));//感知时间
                finalDataList.add(dataList.get(2));//电表在线数
                Object unOnline = dataList.get(3);
                int size = 0;
                StringBuilder sb = null;
                if (unOnline != null && unOnline instanceof List) {
                    List unList = (List) unOnline;
                    size = unList.size();
                    sb = new StringBuilder();
                    for (Object un : unList
                            ) {
                        if (un instanceof List) {
                            Object ob = ((List) un).get(0);
                            sb.append(ob + ",");
                        }
                    }
                    sb.deleteCharAt(sb.length() - 1);
                }
                finalDataList.add(size);//不在线
                finalDataList.add(dataList.get(1));//电表总数
                finalDataList.add(sb == null ? null : sb.toString());
                finalDataList.add("0");//数据电标志 0 288
                finalDataList.add(a + 1);//时间点
                finalDataList.add(tao.getPowerUnitNumber().substring(0, 5));
                finalDataList.add(new Date());
                refreshKey = CommonUtils.refreshKey(tao.getTerminalId(), terminalId, changeDate(DateUtil.format(percDate, DateUtil.defaultDatePattern_YMD)), businessDataitemId, protocolId);
                break;
            //查询网络拓扑
            case "12#221":
                if (terminalId == null) {
                    logger.error("无法从缓存获取正确的档案信息:" + areaCode + "_" + terminalAddr);
                    return;
                }
                int total = (int) dataList.get(0);
                int startIndex = (int) dataList.get(1);
                List dataLists = (List) dataList.get(2);
                if (dataLists == null) {
                    return;
                }
                int anwserNum = dataLists.size();
                for (int d = 0; d < dataLists.size(); d++) {
                    List dList = (List) dataLists.get(d);
                    if (dList == null) {
                        continue;
                    }
                    List dataListFinal = new ArrayList();
                    dataListFinal.add(terminalId);//TERMINAL_ID:BIGINT,
                    dataListFinal.add(0);// TASK_ID:BIGINT,
                    dataListFinal.add(dList.get(0));// NODE_ADDR:STRING,
                    dataListFinal.add(DateUtil.format(new Date(), DateUtil.defaultDatePattern_YMD));// DATA_DATE:STRING,
                    dataListFinal.add(DateUtil.format(new Date(), DateUtil.defaultDatePattern_YMD));// COLL_DATE:STRING,
                    dataListFinal.add(tao.getPowerUnitNumber());// ORG_NO:STRING,
                    dataListFinal.add(null);// CP_NO:STRING,
                    dataListFinal.add(areaCode);// AREA_CODE:STRING,
                    dataListFinal.add(terminalAddr);// TERMINAL_ADDR:STRING,
                    dataListFinal.add(total);// NODE_SNUM:BIGINT,
                    dataListFinal.add(anwserNum);// ANWSER_NUM:BIGINT,
                    dataListFinal.add(d + 1);// NODE_IDEX:BIGINT,
                    dataListFinal.add(startIndex);// START_INDEX:BIGINT,

                    dataListFinal.add(dList.get(1));// NODE_ID:BIGINT,
                    dataListFinal.add(dList.get(2));// PROXY_NODE_ID:BIGINT,

                    int value = Integer.parseInt(dList.get(3).toString());
                    String s = Integer.toBinaryString(value);
                    s = "00000000".substring(0, 8 - s.length()) + s;
                    int level = Integer.parseInt(s.substring(0, 4));
                    int role = Integer.parseInt(s.substring(4, 8));

                    dataListFinal.add(level);// NODE_TIER:BIGINT,
                    dataListFinal.add(role);// NODE_ROLE:BIGINT,

                    dataListFinal.add(null);// NODE_TYPE:BIGINT,
                    dataListFinal.add(null);// PHASE_INFO:BIGINT,
                    dataListFinal.add(null);// LINE_ABNORMAL_ID:BIGINT,
                    dataListFinal.add(null);// VENDOR_CODE:STRING,
                    dataListFinal.add(null);// UPLINK_SUCC_RATE:BIGINT,
                    dataListFinal.add(null);// DOWNLINK_SUCC_RATE:BIGINT,
                    dataListFinal.add(null);// BOOTLOADER_VER:STRING,
                    dataListFinal.add(null);// APPLICATION_VER:STRING,
                    dataListFinal.add(tao.getPowerUnitNumber().substring(0, 5));// SHARD_NO:STRING,
                    dataListFinal.add(new Date());// INSERT_TIME:TIMESTAMP
                    finalDataList.add(dataListFinal);
                }
                refreshKey = CommonUtils.refreshKey(tao.getTerminalId(), terminalId, "00000000000000", businessDataitemId, protocolId);
                break;
            case "12#222":

                if (terminalId == null) {
                    logger.error("无法从缓存获取正确的档案信息:" + areaCode + "_" + terminalAddr);
                    return;
                }

                List jdList = null;
                if (dataList.size() > 3) {
                    jdList = dataList.subList(3, dataList.size());
                }
                String orgNo = tao.getPowerUnitNumber();

                if (jdList == null) {
                    return;
                }
                for (int l = 0; l < jdList.size(); l++) {
                    List dataListFinal_end = new ArrayList();
                    dataListFinal_end.add(terminalId); //TERMINAL_ID:BIGINT,
                    dataListFinal_end.add(jdList.get(l)); // NEIGHBOUR_NET_ID:
                    dataListFinal_end.add(l + 1); // STRING,POINT_INDEX:STRING,
                    dataListFinal_end.add(DateUtil.format(new Date(), DateUtil.defaultDatePattern_YMD));// DATA_DATE:STRING,
                    dataListFinal_end.add(dataList.get(0)); // POINT_NUM:STRING,
                    dataListFinal_end.add(dataList.get(1));  // LOCAL_NET_ID:STRING,
                    dataListFinal_end.add(dataList.get(2)); // LOCAL_MAIN_POINT_ADDR:STRING,
                    dataListFinal_end.add(orgNo.substring(0, 5)); // SHARD_NO:STRING,
                    dataListFinal_end.add(new Date()); // INSERT_TIME:TIMESTAMP
                    finalDataList.add(dataListFinal_end);
                }
                refreshKey = CommonUtils.refreshKey(terminalId, terminalId, "00000000000000", businessDataitemId, protocolId);

                break;
            //Hplc芯片信息
            case "12#223":

                orgNo = tao.getPowerUnitNumber();
                if (terminalId == null || orgNo == null) {
                    logger.error("无法从缓存获取正确的档案信息:" + areaCode + "_" + terminalAddr);
                    return;
                }
                if (dataList.size() < 3) {
                    return;
                }
                total = (int) dataList.get(0);
                startIndex = (int) dataList.get(1);
                dataLists = (List) dataList.get(2);
                if (dataLists == null) {
                    return;
                }
                for (int d = 0; d < dataLists.size(); d++) {
                    List dList = (List) dataLists.get(d);
                    if (dList == null) {
                        continue;
                    }
                    List dataListFinal = new ArrayList<>();
                    dataListFinal.add(dList.get(2));//CHIP_ID:STRING,
                    dataListFinal.add(orgNo);//ORG_NO:STRING,
                    dataListFinal.add(DateUtil.format(new Date(), DateUtil.defaultDatePattern_YMD));//DATA_DATE:STRING,
                    dataListFinal.add(tao.getTerminalId());//TMNLId:STRING,
                    dataListFinal.add(dList.get(1));//EQUIP_TYPE:STRING,
                    dataListFinal.add(dList.get(0));//EQUIP_ADDR:STRING,
                    dataListFinal.add(dList.get(3));//CHIP_SOFT_VER:STRING,
                    dataListFinal.add(new Date());//STORE_DATE:TIMESTAMP,
                    dataListFinal.add(total);//EQUIP_TOTA:BIGINT,
                    dataListFinal.add(d + 1);//NODE_INDEX
                    dataListFinal.add(orgNo.substring(0, 5));//SHARD_NO:STRING
                    finalDataList.add(dataListFinal);
                }
                refreshKey = CommonUtils.refreshKey(tao.getTerminalId(), terminalId, "00000000000000", businessDataitemId, protocolId);

                break;
            //查询相位变更信息
            case "12#210":
            case "12#211":
            case "12#212":
            case "12#213":

                orgNo = tao.getPowerUnitNumber();
                if (terminalId == null || orgNo == null) {
                    logger.error("无法从缓存获取正确的档案信息:" + areaCode + "_" + terminalAddr);
                    return;
                }
                finalDataList.add(new BigDecimal(terminalId));//TERMINAL_ID:BIGINT:本实体记录的唯一标识
                finalDataList.add(DateUtil.format(new Date(), DateUtil.defaultDatePattern_YMD));//DATA_DATE:DATE:数据日期
                finalDataList.add(orgNo);//ORG_NO:VARCHAR:供电单位
                finalDataList.add(dataList.get(0));//PHASE_CHANGE_NUM:INT:变更测量点总数量
                int index = 1;
                if (fn == 210) {
                    finalDataList.add(dataList.get(index));
                    index = 2;
                }
                Object dl = dataList.get(index);
                List pointList = null;
                size = 0;
                if (dl != null && dl instanceof List) {
                    pointList = (List) dl;
                    size = pointList.size();
                }
                finalDataList.add(size);//RESPONSE_NUM:INT:本帧回复的变更测量点数量
                sb = new StringBuilder();
                if (pointList != null) {
                    for (Object os : pointList
                            ) {
                        List point = (List) os;
                        for (int i = 0, s = point.size(); i < s; i++) {
                            sb.append(point.get(i) + ",");
                        }
                        sb.replace(sb.length() - 1, sb.length(), "|");
                    }
                    if (sb.length() != 0) {
                        sb.deleteCharAt(sb.length() - 1);
                    }
                }
                finalDataList.add(sb == null ? null : sb.toString());//CHANNEL_QUALITY:VARCHAR:相位变更信息，存储格式为：第1个测量点号,第1个测量点变更前相位信息 ,第1个测量点变更后相位信息|第2个测量点号,第2个测量点变更前相位信息 ,第2个测量点变更后相位信息|......
                finalDataList.add(orgNo.substring(0, 5));//SHARD_NO:VARCHAR:分库字段
                finalDataList.add(new Date());//INSERT_TIME:DATETIME:插入或更新时间
                refreshKey = CommonUtils.refreshKey(tao.getTerminalId(), terminalId, "00000000000000", businessDataitemId, protocolId);
                break;
            //搜到的电表信息
            case "12#13":
                orgNo = tao.getPowerUnitNumber();
                if (terminalId == null || orgNo == null) {
                    logger.error("无法从缓存获取正确的档案信息:" + areaCode + "_" + terminalAddr);
                    return;
                }
//                        dataList
                List datas = (List) dataList.get(1);
                for (int i = 0; i < datas.size(); i++) {
                    List dataListFinal = new ArrayList();
                    List searchMeter = (List) datas.get(i);
                    dataListFinal.add(new BigDecimal(terminalId));//TERMINAL_ID:BIGINT:终端ID
                    dataListFinal.add(null);//TMNL_BAR_CODE:VARCHAR:集中器资产编号
                    dataListFinal.add(terminalAddr);//TMNL_ADDR:VARCHAR:集中器地址
                    dataListFinal.add(searchMeter.get(0));//COMM_ADDR:VARCHAR:电表地址
                    dataListFinal.add(String.valueOf(searchMeter.get(1)));//COMM_PROTOCOL:VARCHAR:电表规约
                    dataListFinal.add(searchMeter.get(2));//FMR_ADDR:VARCHAR:采集器地址
                    dataListFinal.add(null);//MEASURE_NO:VARCHAR:测量点号
                    dataListFinal.add(DateUtil.format(clock, DateUtil.defaultDatePattern_YMD));//DATA_DATE:DATE:数据日期
                    dataListFinal.add(clock);//REPORT_TIME:DATETIME:上报时间
                    dataListFinal.add(null);//MEMO:VARCHAR:可记录上报的原始关键信息

                    dataListFinal.add(clock);//SEARCH_TIME:DATETIME:搜索到电表时刻
                    dataListFinal.add(i + 1);//INDEX_NUM:TINYINT:搜索序号
                    dataListFinal.add(null);//SIGNAL_QUAL:DECIMAL:信号质量
                    dataListFinal.add(null);//COMM_PHASE:VARCHAR:1：A，2：B，3：C
                    dataListFinal.add(orgNo.substring(0, 5));//SHARD_NO:VARCHAR:分库字段
                    dataListFinal.add(new Date());//INSERT_TIME:DATETIME:插入或更新时间
                    finalDataList.add(dataListFinal);
                }
                break;
            //当前掉电记录数据
            case "12#246":
                if (mpedIdStr == null) {
                    return;
                }
                finalDataList.add(new BigDecimal(mpedIdStr));
                finalDataList.add(DateUtil.format(new Date(), DateUtil.defaultDatePattern_YMD));//dataDate
                finalDataList.add(terminalId);//终端id占位
                finalDataList.add(dataList.get(0));
                for (int i = 1; i < dataList.size(); i++) {
                    finalDataList.add(dataList.get(i));
                }
                finalDataList.add(new Date());//入库时间
                finalDataList.add(tao.getPowerUnitNumber().substring(0, 5));//前闭后开  前5位 不包括[5]
                refreshKey = CommonUtils.refreshKey(tao.getTerminalId(), mpedIdStr, "00000000000000", businessDataitemId, protocolId);
                break;
            //购用电信息
            case "12#167":
                if (mpedIdStr == null) {
                    return;
                }
                finalDataList.add(new BigDecimal(mpedIdStr));
                Date data_date = new Date();
//                if( dataList.get(0)!=null){
//                    try {
//                        data_date= (Date) dataList.get(0);
//                    }catch (Exception e){
//
//                    }
//                }
                finalDataList.add(DateUtil.format(data_date, DateUtil.defaultDatePattern_YMD));//dataDate
                finalDataList.add(dataList.get(0));//抄表日期
                finalDataList.add(tao.getPowerUnitNumber());//orgNo占位
                finalDataList.add(dataList.get(4));//剩余电量
                Object syje = dataList.get(2);//剩余金额
                //6-11 剩余金额为空不入库
                if (syje == null) {
                    return;
                }
                BigDecimal bSyje = new BigDecimal(syje.toString());
                if (bSyje.compareTo(BigDecimal.ZERO) == 0) {
                    BigDecimal p1 = new BigDecimal("0.00");
                    BigDecimal p2 = new BigDecimal(dataList.get(5).toString());
                    finalDataList.add(p1.subtract(p2).doubleValue());//剩余金额
                } else {
                    finalDataList.add(dataList.get(2));//剩余金额
                }

                finalDataList.add(dataList.get(8));//报警电量
                finalDataList.add(dataList.get(9));//故障电量
                finalDataList.add(dataList.get(6));//累计购电量
                finalDataList.add(dataList.get(3));//累计购电金额
                if (dataList.get(1) != null) {
                    finalDataList.add(((Double) dataList.get(1)).intValue());//购电次数
                } else {
                    finalDataList.add(dataList.get(1));//购电次数
                }
                finalDataList.add(dataList.get(7));//赊欠门限值
                finalDataList.add(dataList.get(5));//透支电量
                finalDataList.add(new Date());//入库时间
                finalDataList.add(tao.getPowerUnitNumber().substring(0, 5));//前闭后开  前5位 不包括[5]

                refreshKey = CommonUtils.refreshKey(tao.getTerminalId(), mpedIdStr, changeDate(DateUtil.format(data_date, DateUtil.defaultDatePattern_YMD)), businessDataitemId, protocolId);

                break;
            //当前三相及总有/无功功率，功率因数，三相电压，电流，零序电流，视在功率
            case "12#25":
                orgNo = tao.getPowerUnitNumber();
                Date colTime = (Date) dataList.get(0);
                String eventDate = DateUtil.format(colTime, DateUtil.defaultDatePattern_YMD);
                if (dataList.size() != 24) {
                    return;
                }
                Map<String, List> bodyMap = new HashMap<>(6);
                bodyMap.put("active_power", dataList.subList(1, 5));
                bodyMap.put("reactive_power", dataList.subList(5, 9));
                bodyMap.put("power_factor", dataList.subList(9, 13));
                bodyMap.put("voltage", dataList.subList(13, 16));
                bodyMap.put("electricity", dataList.subList(16, 20));
                bodyMap.put("power", dataList.subList(20, 24));

                for (Map.Entry<String, List> m : bodyMap.entrySet()
                        ) {
                    String key = m.getKey();
                    List body = m.getValue();
                    List newDataList = new ArrayList();
                    newDataList.add(new BigDecimal(mpedIdStr));//ID:BIGINT:测量点标识（MPED_ID）
                    newDataList.add(eventDate);//DATA_DATE:DATE:数据日期
                    newDataList.add(colTime);//COL_TIME:DATETIME:终端抄表日期时间
                    newDataList.add(tao.getPowerUnitNumber());//ORG_NO:VARCHAR:供电单位编号
                    for (int i = 0, j = body.size(); i < j; i++) {
                        newDataList.add(body.get(i));
                    }
                    newDataList.add(orgNo.substring(0, 5));//shardNo
                    newDataList.add(new Date());
                    newDataList.add(key);
                    finalDataList.add(newDataList);
                }
                refreshKey = CommonUtils.refreshKey(tao.getTerminalId(), mpedIdStr, changeDate(eventDate), businessDataitemId, protocolId);

                break;
            //集中抄表电表抄读信息
            case "12#170":
                if (mpedIdStr == null) {
                    return;
                }
                orgNo = tao.getPowerUnitNumber();
                finalDataList.add(new BigDecimal(mpedIdStr));//ID:BIGINT:主键
                eventDate = DateUtil.format(new Date(), DateUtil.defaultDatePattern_YMD);
                finalDataList.add(DateUtil.format((Date) dataList.get(5), DateUtil.defaultDatePattern_YMD));//DATA_DATE:DATE:数据日期
                finalDataList.add(eventDate);//CO_DATE:DATE:采集日期
                finalDataList.add(orgNo);//ORG_NO:VARCHAR:供电单位编号
                finalDataList.add(null);//CP_NO:VARCHAR:采集点编号
                finalDataList.add(areaCode);//AREA_CODE:VARCHAR:区域码
                finalDataList.add(ProtocolArchives.getInstance().getMeterCommAddr(mpedIdStr));//Comm_addr
                finalDataList.add(terminalAddr);//TERMINAL_ADDR:VARCHAR:终端地址码
                finalDataList.add(dataList.get(0));//CMNT_PORT_NO:INT:通信端口号
                finalDataList.add(dataList.get(1));//RELAY_ROUT_SERIES:INT:中继路由级数
                String phaseMessage = new StringBuilder((String) dataList.get(2)).reverse().toString();
                String quality = (String) dataList.get(3);
                char[] chars = phaseMessage.toCharArray();
                finalDataList.add(chars[4]);//READ_PHASE_A:VARCHAR:存载波相位信息的D4位，1为A相接入，0为A相未接入或断相
                finalDataList.add(chars[5]);//READ_PHASE_B:VARCHAR:存载波相位信息的D5位，1为B相接入，0为B相未接入或断相
                finalDataList.add(chars[6]);//READ_PHASE_C:VARCHAR:存载波相位信息的D6位，1为C相接入，0为C相未接入或断相
                finalDataList.add(phaseMessage.substring(4, 7));//READ_PHASE:VARCHAR:存载波相位信息的D4-D6位
                finalDataList.add(chars[7]);//LINE_ABNORMAL_ID:VARCHAR:存载波相位信息的D7位，0表示接线正常，1表示接线异常，零火互易
                finalDataList.add(null);//ACTUAL_PHASE_A:VARCHAR:实际相位A
                finalDataList.add(null);//ACTUAL_PHASE_B:VARCHAR:实际相位B
                finalDataList.add(null);//ACTUAL_PHASE_C:VARCHAR:实际相位C
                finalDataList.add(null);//ACTUAL_PHASE:VARCHAR:实际相位
                finalDataList.add(quality.substring(0, 5));//SEND_QUALITY:VARCHAR:存载波信号品质的D7-D4位，可翻译成数字范围1-15
                finalDataList.add(quality.substring(5));//RCV_QUALITY:VARCHAR:存载波信号品质的D3-D0位，可翻译成数字范围1-15
                finalDataList.add(dataList.get(4));//LAST_READ_FLAG:VARCHAR:最近一次抄表成功/失败标志
                finalDataList.add(dataList.get(5));//LAST_READ_SUCC_TIME:DATETIME:最近一次抄表成功时间
                finalDataList.add(dataList.get(6));//LAST_READ_FAIL_TIME:DATETIME:最近一次抄表失败时间
                finalDataList.add(dataList.get(7));//LAST_FAIL_TIMES:INT:最近连续失败累计次数
                finalDataList.add(quality.substring(5, 6));//METER_PHASE:VARCHAR:载波表相别,存载波相位信息的D3位：0代表单相载波表? 1代表三相载波表
                finalDataList.add(quality.substring(6));//SEQUENTIAL_TYPE:VARCHAR:相序类型,存载波相位信息的D2-D0位：000表示ABC正常相位，001表示ACB，010表示BAC，011表示BCA，100表示CAB，101表示CBA，110表示零火反接，111为保留
                finalDataList.add(new Date());//入库时间
                finalDataList.add(orgNo.substring(0, 5));//前闭后开  前5位 不包括[5]

                refreshKey = CommonUtils.refreshKey(tao.getTerminalId(), mpedIdStr, changeDate(DateUtil.format((Date) dataList.get(5), DateUtil.defaultDatePattern_YMD)), businessDataitemId, protocolId);

                break;
            //查询网络规模
            case "12#258":
                if (terminalId == null) {
                    return;
                }
                finalDataList.add(new BigDecimal(terminalId));//TERMINAL_ID:BIGINT:终端ID
                finalDataList.add(DateUtil.format(new Date(), DateUtil.defaultDatePattern_YMD));//DATA_DATE:DATE:数据日期
                finalDataList.add(dataList.get(0));//NETWORK_SIZE:INT:网络规模
                if(ParamConstants.startWith.equals("11")) {
                    finalDataList.add(new Date());//入库时间
                    finalDataList.add(tao.getPowerUnitNumber().substring(0, 5));//前闭后开  前5位 不包括[5]

                }else{
                    finalDataList.add(tao.getPowerUnitNumber().substring(0, 5));//前闭后开  前5位 不包括[5]
                    finalDataList.add(new Date());//入库时间
                }
                refreshKey = CommonUtils.refreshKey(tao.getTerminalId(), terminalId, "00000000000000", businessDataitemId, protocolId);
                break;
            //查询指定测量点当前相位信息
            case "12#214":
                orgNo = tao.getPowerUnitNumber();
                finalDataList.add(mpedIdStr);//MPED_ID:BIGINT:测量点ID
                finalDataList.add(terminalId);//TERMINAL_ID:BIGINT:终端ID
                finalDataList.add(dataList.get(0));//INFORMATION_TYPE:BIGINT:信息类型：1-当前相位信息，2-当前版本信息，3-当前 ID 信息
                finalDataList.add(dataList.get(1));//INFORMATION_LENGTH:BIGINT:信息长度
                byte[] bts = (byte[]) dataList.get(2);
                sb = new StringBuilder();
                for (int i = 0; i < bts.length; i++) {
                    sb.append(bts[i]);
                }
                finalDataList.add(sb.toString());//INFORMATION_CONTENT:BIGINT:信息内同
                finalDataList.add(orgNo.substring(0, 5));//SHARD_NO:VARCHAR:分库字段
                finalDataList.add(new Date());//INSERT_TIME:DATETIME:插入时间
                refreshKey = CommonUtils.refreshKey(tao.getTerminalId(), terminalId, "00000000000000", businessDataitemId, protocolId);
                break;
            //电池状态
            case "12#247":
                if (terminalId == null) {
                    logger.error("无法从缓存获取正确的档案信息:" + areaCode + "_" + terminalAddr);
                    return;
                }

                finalDataList.add(BigDecimal.valueOf(Long.valueOf(terminalId)));
                finalDataList.add(DateUtil.format(new Date(), DateUtil.defaultDatePattern_YMD));
                finalDataList.add(tao.getPowerUnitNumber());
                if (ParamConstants.startWith.equals("11")) {
                    finalDataList.add(null);
                    finalDataList.add(dataList.get(0));
                }else {
                    finalDataList.add(dataList.get(0));
                    finalDataList.add(null);
                }
                finalDataList.add(tao.getPowerUnitNumber().substring(0, 5));
                finalDataList.add(new Date());
                refreshKey = CommonUtils.refreshKey(tao.getTerminalId(), terminalId, "00000000000000", businessDataitemId, protocolId);
                break;
            case "12#15":
                if (dataList.size() < 2) {
                    return;
                }
                List alldateList = (List) dataList.get(1);
                if (alldateList.size() == 0) {
                    return;
                }
                for (int i = 0; i < alldateList.size(); i++) {
                    try {
                        dataFinalList = new ArrayList();
                        dtList = (List) alldateList.get(i);
                        String commaddr = dtList.get(0).toString();
                        tao = TerminalArchives.getInstance().getTmnlAndMpedIdArchives(areaCode, String.valueOf(terminalAddr), "COMMADDR" + commaddr);
                        if (tao == null) {
                            continue;
                        }
                        String mpedId = tao.getID();
                        if (terminalId == null || mpedId == null) {
                            continue;
                        }
                        dataFinalList.add(terminalId);//TERMINAL_ID:BIGINT:终端设备标识
                        if (ParamConstants.startWith.equals("11")){
                            String NODE_IDEX = TerminalArchives.getInstance().getPnFromMpedId(mpedId);
                            logger.info(NODE_IDEX);
                            dataFinalList.add(NODE_IDEX);//NODE_IDEX:TINYINT:节点序号,0:主节点，1 - n:从节点
                        }else{
                            dataFinalList.add(i);
                        }

                        dataFinalList.add(areaCode);//AREA_CODE:VARCHAR:区域码
                        dataFinalList.add(terminalAddr);//TERMINAL_ADDR:VARCHAR:终端地址码
                        dataFinalList.add(mpedId);//MPED_ID:BIGINT:当节点序号为0时为终端id，当不为0时需要根据终端id和节点地址获取mped_id
                        dataFinalList.add(DateUtil.format(new Date(), DateUtil.defaultDatePattern_YMD));//DATA_DATE:DATE:数据日期
                        dataFinalList.add(commaddr);//NODE_ADDR:VARCHAR:节点地址,当为从节点时节点地址为电表地址
                        dataFinalList.add(dtList.get(1));//SOFTWARE_VERSION_NO:VARCHAR:软件版本号
                        String rq = "20" + dtList.get(4) + "-" + dtList.get(3) + "-" + dtList.get(2);
                        dataFinalList.add(rq);//SOFTWARE_VERSION_DATE:DATE:软件版本日期

                        dataFinalList.add(dtList.get(5));//MODULE_VENDOR_CODE:VARCHAR:模块厂商代码
                        dataFinalList.add(dtList.get(6));//CHIP_CODE:VARCHAR:芯片代码
                        dataFinalList.add(tao.getPowerUnitNumber().substring(0, 5));//SHARD_NO:VARCHAR:分库字段
                        dataFinalList.add(new Date());//INSERT_TIME:DATETIME:插入或更新时间
                        finalDataList.add(dataFinalList);
                    } catch (Exception e) {
                        logger.error(e.toString());
                    }
                }
                refreshKey = CommonUtils.refreshKey(tao.getTerminalId(), terminalId, "00000000000000", businessDataitemId, protocolId);
                break;
            case "10#9":
                if(ParamConstants.startWith.equals("41")){
                    dataFinalList.add(terminalId);
                    dataFinalList.add(dataList.get(0));
                    dataFinalList.add(dataList.get(1));
                    dataFinalList.add(tao.getPowerUnitNumber().substring(0, 5));
                    dataFinalList.add(new Date());
                    refreshKey = CommonUtils.refreshKey(tao.getTerminalId(), terminalId, "00000000000000", businessDataitemId, protocolId);
                }
                break;
            case "10#8": //终端上行通信工作方式
                finalDataList.add(BigDecimal.valueOf(Long.valueOf(terminalId)));
                finalDataList.add(DateUtil.format(new Date(), DateUtil.defaultDatePattern_YMD));
                finalDataList.add(terminalAddr);
                finalDataList.add(areaCode);
                finalDataList.add(dataList.get(0));
                finalDataList.add(dataList.get(1));
                finalDataList.add(dataList.get(2));
                finalDataList.add(dataList.get(3));
                Object o = dataList.get(4);
                String timeFlag = String.valueOf(o);
                String start = timeFlag.substring(0, 8);
                String center = timeFlag.substring(8, timeFlag.length() - 8);
                String end = timeFlag.substring(timeFlag.length() - 8);
                finalDataList.add(end + center + start);
                orgNo = tao.getPowerUnitNumber();
                finalDataList.add(orgNo.substring(0, 5));
                finalDataList.add(new Date());
                refreshKey = CommonUtils.refreshKey(tao.getTerminalId(), terminalId, "00000000000000", businessDataitemId, protocolId);

                break;
            case "12#173":
                //白名单开启/关闭状态
                finalDataList.add(BigDecimal.valueOf(Long.valueOf(terminalId)));
                finalDataList.add(DateUtil.format(new Date(), DateUtil.defaultDatePattern_YMD));
                finalDataList.add(terminalAddr);//TERMINAL_ADDR:VARCHAR:终端通信的地址码信息。
                finalDataList.add(areaCode);//AREA_CODE:INT:行政区划码
                finalDataList.add(dataList.get(0));//WHITE_LIST_STATUS:VARCHAR:白名单开启/关闭状态,0:关闭,1:开启
                finalDataList.add(tao.getPowerUnitNumber().substring(0, 5));//SHARD_NO:VARCHAR:分库字段
                finalDataList.add(new Date());//INSERT_TIME:DATETIME:插入或更新时间
                refreshKey = CommonUtils.refreshKey(tao.getTerminalId(), terminalId, "00000000000000", businessDataitemId, protocolId);
                break;
            default:
                if (afn == 12) {
                    boolean allNull = CommonUtils.allNull(dataList);
                    if (allNull) {//对每个数据监测，若为空或date或int，则list为空，执行下一个测量点
                        logger.error("data " + afn + "_" + fn + " is allnull");
                        return;
                    }
                    if (fn >= 129 && fn <= 136) {
                        finalDataList.add(1);//从缓存取测量点标识，这里占位
                        switch (fn) {
                            case 129:
                                finalDataList.add("1");//1正向有功
                                break;
                            case 130:
                                finalDataList.add("2");//正向无功
                                break;
                            case 131:
                                finalDataList.add("5");//反向有功
                                break;
                            case 132:
                                finalDataList.add("6");//反向无功
                                break;
                            case 133:
                                finalDataList.add("3");//一象限无功
                                break;
                            case 134:
                                finalDataList.add("4");//二象限无功
                                break;
                            case 135:
                                finalDataList.add("7");//三象限无功
                                break;
                            case 136:
                                finalDataList.add("8");//四象限无功
                                break;
                            default:
                                finalDataList.add("0");//备用
                        }

                        int m = (Integer) dataList.remove(1);//费率个数m  被删除后，后面的往前平移1
                        List list = (List) ((List) dataList.get(2)).get(0);//m个 (m1的正向有功电能示值,m2,m3,m4)
                        Object colDate = dataList.get(0);
                        if (null != colDate && colDate instanceof Date) {
                            finalDataList.add(colDate);
                        }
                        finalDataList.add(dataList.get(1)); //正向有功总电能示值
                        finalDataList.addAll(list);//4个值的list，一次性添加
                        for (int i = m; i < 14; i++) {//补充14-m个正向有功电能示值
                            finalDataList.add(null);
                        }
                        finalDataList.add(1, mpedIdStr);//首元素id
                        // 把数据时标放在最后，方便外层处理
                        Object dataDate = colDate;
                        if (null != dataDate && dataDate instanceof Date) {
                            finalDataList.add(DateUtil.format(DateUtil.addDaysOfMonth((Date) dataDate, -1), DateUtil.defaultDatePattern_YMD)); //日冻结类数据时标Td_d(数据日期)，取当前采集数据日期-1
                        }
                        finalDataList.set(0, mpedIdStr + "_" + finalDataList.get(finalDataList.size() - 1));
                        finalDataList.add(tao.getPowerUnitNumber());//供电单位编号
                        finalDataList.add("00");//未分析
                        if ("auto".equals(sType)) {
                            data_src = "10";
                        } else {
                            data_src = "14";
                        }
                        finalDataList.add(data_src);//自动抄表
                        finalDataList.add(new Date());//入库时间
                        finalDataList.add(tao.getPowerUnitNumber().substring(0, 5));//前闭后开  前5位 不包括[5]
                        finalDataList.add(finalDataList.size(), finalDataList.get(finalDataList.size() - 6));
                        finalDataList.remove(finalDataList.size() - 7);
                        refreshKey = CommonUtils.refreshKey(tao.getTerminalId(), mpedIdStr, changeDate(DateUtil.format(DateUtil.addDaysOfMonth((Date) dataDate, 0), DateUtil.defaultDatePattern_YMD)), businessDataitemId, protocolId);
                    }
                }
        }


        if (finalDataList.size() < 1) {
            return;
        }
        if (!"auto".equals(sType)) {
            refreshKey = null;
        }
        if (itemList.contains(businessDataitemId) && "auto".equals(sType)) {
            businessDataitemId = "current";
        }
        if (finalDataList.get(0) instanceof List) {
            for (int i = 0, j = finalDataList.size(); i < j; i++) {
                List df = (List) finalDataList.get(i);
                if (refreshKey != null && i != 0) {
                    refreshKey = null;
                }
                if ("12#25".equals(afnAndfn)) {
                    businessDataitemId = (String) df.remove(df.size() - 1);
                }
                CommonUtils.putToDataHub(businessDataitemId, terminalId, df, refreshKey, listDataObj);
            }
        } else {
            CommonUtils.putToDataHub(businessDataitemId, terminalId, finalDataList, refreshKey, listDataObj);
        }

    }

    private static ThreadLocal<Calendar> currentCalendar = new ThreadLocal<Calendar>();

    private static Calendar getCalendar() {
        Calendar calendar = currentCalendar.get();
        if (null == calendar) {
            calendar = Calendar.getInstance();
            currentCalendar.set(calendar);
        }
        return calendar;
    }

    private static Date changeDate(String d) throws ParseException {
        return DateUtil.parse(d);
    }
}
