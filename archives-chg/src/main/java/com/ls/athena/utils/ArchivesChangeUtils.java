package com.ls.athena.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.ls.pf.common.dataCatch.bz.bo.ObjectBz;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

/**
 * @author Dongwei-Chen
 * @Date 2021/8/25 15:37
 * @Description query data  Util
 */
@Component
public class ArchivesChangeUtils {

    private final static Logger logger = LoggerFactory.getLogger(ArchivesChangeUtils.class);

    @Value("${chg.versions}")
    private String versions;

    private static String version;

    private static String GET_MPED_INFO_SQL;
    private static String GET_IOT_MPED_INFO_SQL;
    private static String GET_IOT_SP_MPED_INFO_SQL;
    private static String GET_TMNL_RUN_INFO_SQL;
    private static String GET_MPED_INFO_SQL_SQR;
    private static String GET_READ_UNEVEN = " select PARAM_VALUE from sp_sysparam where PARAM_CODE in ('READ_UNEVEN') ";
    private static String GET_READ_FLY = " select PARAM_VALUE from sp_sysparam where PARAM_CODE in ('READ_FLY') ";
    private static String SEPARATOR = "#";

    @PostConstruct
    private void post() {
        version = versions;
        GET_MPED_INFO_SQL = getMpedInfoSql();
        GET_IOT_MPED_INFO_SQL = getIotMpedInfoSql();
        GET_IOT_SP_MPED_INFO_SQL = getIotSpMpedInfoSql();
        GET_TMNL_RUN_INFO_SQL = getTmnlRunInfoSql();
        GET_MPED_INFO_SQL_SQR = getMpedInfoSqlSqr();
    }

    private ArchivesChangeUtils() {
    }

    private static final String GET_OTS_INFO_SQL(String readFly) {
        return getOtsInfoSql(readFly);
    }

    /**
     * 获取写入OTS档案信息字段
     *
     * @throws Exception
     */
    public static List<Map<String, String>> getListToOts(JdbcTemplate jdbcTemplate, List<String> list) throws Exception {
        SqlRowSet rs1 = jdbcTemplate.queryForRowSet(GET_READ_FLY);
        String readFly = "";
        String readUneven = "";
        while (rs1.next()) {
            readFly = rs1.getString("PARAM_VALUE");
        }
        SqlRowSet rs2 = jdbcTemplate.queryForRowSet(GET_READ_UNEVEN);
        while (rs2.next()) {
            readUneven = rs2.getString("PARAM_VALUE");
        }
        SqlRowSet rs = jdbcTemplate.queryForRowSet(GET_OTS_INFO_SQL(readFly), new String[]{list.get(0), list.get(1), list.get(0), list.get(1)});
        List<Map<String, String>> otsList = new ArrayList<>();
        while (rs.next()) {
            HashMap<String, String> map = new HashMap<String, String>();
            map.put("mped_id", rs.getString(1));
            map.put("thr_fz", rs.getString(2));
            map.put("multirate_falg", rs.getString(3));
            map.put("para_status", rs.getString(5));
            String debug_time = rs.getString(6) == null ? "" : rs.getString(6);
            map.put("debug_time", debug_time);
            map.put("thr_bp", readUneven);
            otsList.add(map);
        }
        return otsList;
    }

    /**
     * 获取终端及测量点信息
     *
     * @throws Exception
     */
    public static List<ObjectBz> getTerminalInfo(JdbcTemplate jdbcTemplate, List<String> list) throws Exception {
        if (list == null || list.size() == 0) { // 没有删除或者没有更新的档案
            return null;
        }
        list.add(list.get(0));//TERMINAL_ID
        list.add(list.get(1));//SHARD_NO
        List<ObjectBz> doclist = new ArrayList<>();
        ObjectBz objBz;
        SqlRowSet rs = jdbcTemplate.queryForRowSet(GET_TMNL_RUN_INFO_SQL, list.toArray());
        while (rs.next()) {
            objBz = new ObjectBz();
            objBz.setAreaCode(rs.getString("AREA_CODE"));
            objBz.setTermAdd(rs.getString("TERMINAL_ADDR"));
            objBz.setTmnlId(rs.getString("TERMINAL_ID"));
            objBz.setDepartCode(rs.getString("ORG_NO"));
            objBz.setProtocolId(rs.getString("PROTOCOL_ID"));
            objBz.setSchemeNo(rs.getString("SCHEME_NO"));
            logger.info("GET_MPED_INFO_SQL:" + GET_MPED_INFO_SQL + " TERMINAL_ID:" + list.get(0));
            SqlRowSet rs2 = jdbcTemplate.queryForRowSet(GET_MPED_INFO_SQL, list.toArray());
            Map<String, String> pointNum2Map = new HashMap<>();//P+(测量点序号)
            Map<String, String> commAddr2Map = new HashMap<>();//COMMADDR+电表地址
            Map<String, String> meterIdMap = new HashMap<>();//MI+电表地址   电能表事件通过表地址取meter_id用
            Map<String, String> userFlagMap = new HashMap<>();
            List<Map<String, String>> meterAttributeList = new ArrayList<>();
            // 2016-7-6杭州start 多规约测量点档案缓存
            List<Map<String, String>> meterMPList = new ArrayList<>();
            while (rs2.next()) {
                String mpedIndex = rs2.getString("MPED_INDEX");
                if (StringUtil.isBlank(mpedIndex)) {
                    continue;
                }
                String mpedId = rs2.getString("MPED_ID");
                if (StringUtil.isBlank(mpedId)) {
                    continue;
                }
                pointNum2Map.put(mpedIndex, mpedId);
                String commAddr = rs2.getString("COMM_ADDR");
                if (commAddr != null && !"".equals(commAddr)) {
                    //FIXME 2022-04-22 光伏模拟档案加载  12位以下补零12位以上不补零
                    int length = commAddr.length();
                    StringBuilder sb = new StringBuilder();
                    if (length < 12) {
                        for (int i = 0; i < 12 - length; i++) {
                            sb.append("0");
                        }
                    }
                    sb.append(commAddr);
                    commAddr = sb.toString();
                    commAddr2Map.put(commAddr, mpedId);
                }
                String meterId = rs2.getString("METER_ID");
                if (meterId != null && !meterId.equals("")) {
                    meterIdMap.put(commAddr, meterId);
                }
                String userFlag = rs2.getString("USERFLAG");
                userFlagMap.put(mpedIndex, userFlag);
                Map<String, String> meterAttribute = new HashMap<>();
                meterAttribute.put("ADDR" + SEPARATOR + mpedIndex, commAddr);
                meterAttribute.put("STOPBITS" + SEPARATOR + mpedIndex, rs2.getString("STOPBITS"));
                meterAttribute.put("PARITY" + SEPARATOR + mpedIndex, rs2.getString("PARITY"));
                meterAttribute.put("BYTESIZE" + SEPARATOR + mpedIndex, rs2.getString("BYTESIZE"));
                meterAttribute.put("BAUDRATE" + SEPARATOR + mpedIndex, rs2.getString("BAUD"));
                meterAttribute.put("PORT" + SEPARATOR + mpedIndex, rs2.getString("PORT_NO"));
                meterAttribute.put("MPT" + SEPARATOR + mpedIndex, rs2.getString("IMPORTANT"));
                meterAttribute.put("MPID" + SEPARATOR + mpedIndex, rs2.getString("PROTOCOL_ID"));
                String tgId = rs2.getString("TG_ID");
                if (tgId != null && !tgId.equals("")) {
                    meterAttribute.put("TGID" + SEPARATOR + mpedIndex, tgId);
                }
                String lineId = rs2.getString("LINE_ID");
                if (lineId != null && !lineId.equals("")) {
                    meterAttribute.put("LINEID" + SEPARATOR + mpedIndex, lineId);
                }
                meterAttributeList.add(meterAttribute);
                // 2016-7-6杭州start 多规约测量点档案缓存MP$**#**
                Map<String, String> meterMPAttribute = new HashMap<>();
                //param 波特率|停止位|无/有校验|偶/奇校验|5-8|位数（无）|报文超时时间单位|报文超时时间
                String param = rs2.getString("BAUD") + "|" + rs2.getString("STOPBITS") + "|"
                        + rs2.getString("CHECK_FLAG") + "|" + rs2.getString("PARITY") + "|"
                        + rs2.getString("BYTESIZE") + "|" + "1" + "|" + rs2.getString("LIMIT_TIME") + "|" + "12000";
                meterMPAttribute.put("MPINDEX" + SEPARATOR + mpedId, rs2.getString("MPED_INDEX"));
                meterMPAttribute.put("ORG" + SEPARATOR + mpedId, rs2.getString("ORG_NO"));
                meterMPAttribute.put("ADDR" + SEPARATOR + mpedId, commAddr);
                meterMPAttribute.put("PTYPE" + SEPARATOR + mpedId, "1");
                meterMPAttribute.put("PTL" + SEPARATOR + mpedId, rs2.getString("PROTOCOL_ID"));
                meterMPAttribute.put("PARAM" + SEPARATOR + mpedId, param);
                meterMPAttribute.put("PORT" + SEPARATOR + mpedId, rs2.getString("PORT_NO"));
                meterMPAttribute.put("TMID" + SEPARATOR + mpedId, rs2.getString("TERMINAL_ID"));
                meterMPAttribute.put("MPT" + SEPARATOR + mpedId, rs2.getString("IMPORTANT"));
                meterMPList.add(meterMPAttribute);
            }
            /*************水汽热start**************/
            logger.info("GET_MPED_INFO_SQL_SQR:" + GET_MPED_INFO_SQL_SQR + " TERMINAL_ID:" + list.get(0));
            SqlRowSet rs4 = jdbcTemplate.queryForRowSet(GET_MPED_INFO_SQL_SQR, new String[]{list.get(0), list.get(1), list.get(1)});
            while (rs4.next()) {
                String mpedIndex = rs4.getString("MPED_INDEX");
                if (StringUtil.isBlank(mpedIndex)) {
                    continue;
                }
                String mpedId = rs4.getString("MPED_ID");
                if (StringUtil.isBlank(mpedId)) {
                    continue;
                }
                String commAddr = rs4.getString(3);
                String userFlag = rs4.getString("USERFLAG");
                userFlagMap.put(mpedIndex, userFlag);
                Map<String, String> meterAttribute = new HashMap<String, String>();
                meterAttribute.put("ADDR" + SEPARATOR + mpedIndex, commAddr);
                meterAttribute.put("STOPBITS" + SEPARATOR + mpedIndex, rs4.getString(10));
                meterAttribute.put("PARITY" + SEPARATOR + mpedIndex, rs4.getString(9));
                meterAttribute.put("BYTESIZE" + SEPARATOR + mpedIndex, rs4.getString(8));
                meterAttribute.put("BAUDRATE" + SEPARATOR + mpedIndex, rs4.getString("BAUD"));
                meterAttribute.put("PORT" + SEPARATOR + mpedIndex, rs4.getString("PORT_NO"));
                meterAttribute.put("MPT" + SEPARATOR + mpedIndex, rs4.getString(1));
                meterAttribute.put("MPID" + SEPARATOR + mpedIndex, rs4.getString("PROTOCOL_ID"));
                meterAttributeList.add(meterAttribute);
                // 2016-7-6杭州start 多规约测量点档案缓存MP$**#**
                Map<String, String> meterMPAttribute = new HashMap<>();
                //param 波特率|停止位|无/有校验|偶/奇校验|5-8|位数（无）|报文超时时间单位|报文超时时间
                String param = rs4.getString("BAUD") + "|" + rs4.getString(10) + "|"
                        + rs4.getString(13) + "|" + rs4.getString(9) + "|"
                        + rs4.getString(8) + "|" + "1" + "|" + rs4.getString(14) + "|" + "12000";
                meterMPAttribute.put("MPINDEX" + SEPARATOR + mpedId, rs4.getString("MPED_INDEX"));
                meterMPAttribute.put("ORG" + SEPARATOR + mpedId, rs4.getString("ORG_NO"));
                meterMPAttribute.put("ADDR" + SEPARATOR + mpedId, rs4.getString(3));
                meterMPAttribute.put("PTYPE" + SEPARATOR + mpedId, "1");
                meterMPAttribute.put("PTL" + SEPARATOR + mpedId, rs4.getString("PROTOCOL_ID"));
                meterMPAttribute.put("PARAM" + SEPARATOR + mpedId, param);
                meterMPAttribute.put("PORT" + SEPARATOR + mpedId, rs4.getString("PORT_NO"));
                meterMPAttribute.put("TMID" + SEPARATOR + mpedId, rs4.getString("TERMINAL_ID"));
                meterMPAttribute.put("MPT" + SEPARATOR + mpedId, rs4.getString(1));
                meterMPList.add(meterMPAttribute);
            }
            /*************水汽热end**************/
            objBz.setPointNum(pointNum2Map);
            objBz.setCommAddr(commAddr2Map);
            objBz.setMeterIdMap(meterIdMap);
            objBz.setUserFlag(userFlagMap);
            objBz.setMeterAttribute(meterAttributeList);
            objBz.setMeterMP(meterMPList);//MP$+测量点业务标识(mped_id) 2016-7-6杭州start 多规约测量点档案缓存
            Map<String, String> portNum2Map = new HashMap<>();//D+(端口号)
            portNum2Map.put("1", "1");
            objBz.setPortNum(portNum2Map);
            doclist.add(objBz);
        }
        return doclist;
    }


    /**
     * @author Dongwei-Chen
     * @Date 2022/7/20 14:33
     * @Description 物联网表
     */
    public static List<ObjectBz> getIotTerminalInfo(JdbcTemplate jdbcTemplate, List<String> list) throws Exception {
        if (list == null || list.size() == 0) { // 没有删除或者没有更新的档案
            return null;
        }
        list.add(list.get(0));//TERMINAL_ID
        list.add(list.get(1));//SHARD_NO
        List<ObjectBz> doclist = new ArrayList<>();
        ObjectBz objBz;
        SqlRowSet rs = jdbcTemplate.queryForRowSet(GET_TMNL_RUN_INFO_SQL, list.toArray());
        while (rs.next()) {
            objBz = new ObjectBz();
            objBz.setAreaCode(rs.getString("AREA_CODE"));
            objBz.setTermAdd(rs.getString("TERMINAL_ADDR"));
            objBz.setTmnlId(rs.getString("TERMINAL_ID"));
            objBz.setDepartCode(rs.getString("ORG_NO"));
            objBz.setProtocolId(rs.getString("PROTOCOL_ID"));
            objBz.setSchemeNo(rs.getString("SCHEME_NO"));
            logger.info("GET_MPED_INFO_SQL:" + GET_IOT_MPED_INFO_SQL + " TERMINAL_ID:" + list.get(0));
            SqlRowSet rs2 = jdbcTemplate.queryForRowSet(GET_IOT_MPED_INFO_SQL, list.toArray());
            Map<String, String> pointNum2Map = new HashMap<>();//P+(测量点序号)
            Map<String, String> commAddr2Map = new HashMap<>();//COMMADDR+电表地址
            Map<String, String> meterIdMap = new HashMap<>();//MI+电表地址   电能表事件通过表地址取meter_id用
            Map<String, String> userFlagMap = new HashMap<>();
            List<Map<String, String>> meterAttributeList = new ArrayList<>();
            // 2016-7-6杭州start 多规约测量点档案缓存
            List<Map<String, String>> meterMPList = new ArrayList<>();
            mpedArgsLoad(rs2, pointNum2Map, commAddr2Map, meterIdMap, userFlagMap, meterAttributeList, meterMPList);
            SqlRowSet iotMped = jdbcTemplate.queryForRowSet(GET_IOT_SP_MPED_INFO_SQL, new String[]{list.get(0),list.get(1)});
            mpedArgsLoad(iotMped, pointNum2Map, commAddr2Map, meterIdMap, userFlagMap, meterAttributeList, meterMPList);

            /*************水汽热start**************/
            logger.info("GET_MPED_INFO_SQL_SQR:" + GET_MPED_INFO_SQL_SQR + " TERMINAL_ID:" + list.get(0));
            SqlRowSet rs4 = jdbcTemplate.queryForRowSet(GET_MPED_INFO_SQL_SQR, new String[]{list.get(0), list.get(1), list.get(1)});
            while (rs4.next()) {
                String mpedIndex = rs4.getString("MPED_INDEX");
                if (StringUtil.isBlank(mpedIndex)) {
                    continue;
                }
                String mpedId = rs4.getString("MPED_ID");
                if (StringUtil.isBlank(mpedId)) {
                    continue;
                }
                String commAddr = rs4.getString(3);
                String userFlag = rs4.getString("USERFLAG");
                userFlagMap.put(mpedIndex, userFlag);
                Map<String, String> meterAttribute = new HashMap<String, String>();
                meterAttribute.put("ADDR" + SEPARATOR + mpedIndex, commAddr);
                meterAttribute.put("STOPBITS" + SEPARATOR + mpedIndex, rs4.getString(10));
                meterAttribute.put("PARITY" + SEPARATOR + mpedIndex, rs4.getString(9));
                meterAttribute.put("BYTESIZE" + SEPARATOR + mpedIndex, rs4.getString(8));
                meterAttribute.put("BAUDRATE" + SEPARATOR + mpedIndex, rs4.getString("BAUD"));
                meterAttribute.put("PORT" + SEPARATOR + mpedIndex, rs4.getString("PORT_NO"));
                meterAttribute.put("MPT" + SEPARATOR + mpedIndex, rs4.getString(1));
                meterAttribute.put("MPID" + SEPARATOR + mpedIndex, rs4.getString("PROTOCOL_ID"));
                meterAttributeList.add(meterAttribute);
                // 2016-7-6杭州start 多规约测量点档案缓存MP$**#**
                Map<String, String> meterMPAttribute = new HashMap<>();
                //param 波特率|停止位|无/有校验|偶/奇校验|5-8|位数（无）|报文超时时间单位|报文超时时间
                String param = rs4.getString("BAUD") + "|" + rs4.getString(10) + "|"
                        + rs4.getString(13) + "|" + rs4.getString(9) + "|"
                        + rs4.getString(8) + "|" + "1" + "|" + rs4.getString(14) + "|" + "12000";
                meterMPAttribute.put("MPINDEX" + SEPARATOR + mpedId, rs4.getString("MPED_INDEX"));
                meterMPAttribute.put("ORG" + SEPARATOR + mpedId, rs4.getString("ORG_NO"));
                meterMPAttribute.put("ADDR" + SEPARATOR + mpedId, rs4.getString(3));
                meterMPAttribute.put("PTYPE" + SEPARATOR + mpedId, "1");
                meterMPAttribute.put("PTL" + SEPARATOR + mpedId, rs4.getString("PROTOCOL_ID"));
                meterMPAttribute.put("PARAM" + SEPARATOR + mpedId, param);
                meterMPAttribute.put("PORT" + SEPARATOR + mpedId, rs4.getString("PORT_NO"));
                meterMPAttribute.put("TMID" + SEPARATOR + mpedId, rs4.getString("TERMINAL_ID"));
                meterMPAttribute.put("MPT" + SEPARATOR + mpedId, rs4.getString(1));
                meterMPList.add(meterMPAttribute);
            }
            /*************水汽热end**************/
            objBz.setPointNum(pointNum2Map);
            objBz.setCommAddr(commAddr2Map);
            objBz.setMeterIdMap(meterIdMap);
            objBz.setUserFlag(userFlagMap);
            objBz.setMeterAttribute(meterAttributeList);
            objBz.setMeterMP(meterMPList);//MP$+测量点业务标识(mped_id) 2016-7-6杭州start 多规约测量点档案缓存
            Map<String, String> portNum2Map = new HashMap<>();//D+(端口号)
            portNum2Map.put("1", "1");
            objBz.setPortNum(portNum2Map);
            doclist.add(objBz);
        }
        return doclist;
    }

    private static void mpedArgsLoad(SqlRowSet rs2, Map<String, String> pointNum2Map, Map<String, String> commAddr2Map, Map<String, String> meterIdMap, Map<String, String> userFlagMap, List<Map<String, String>> meterAttributeList, List<Map<String, String>> meterMPList) {
        while (rs2.next()) {
            String mpedIndex = rs2.getString("MPED_INDEX");
            if (StringUtil.isBlank(mpedIndex)) {
                continue;
            }
            String mpedId = rs2.getString("MPED_ID");
            if (StringUtil.isBlank(mpedId)) {
                continue;
            }
            pointNum2Map.put(mpedIndex, mpedId);
            String commAddr = rs2.getString("COMM_ADDR");
            if (commAddr != null && !"".equals(commAddr)) {
                //FIXME 2022-04-22 光伏模拟档案加载  12位以下补零12位以上不补零
                int length = commAddr.length();
                StringBuilder sb = new StringBuilder();
                if (length < 12) {
                    for (int i = 0; i < 12 - length; i++) {
                        sb.append("0");
                    }
                }
                sb.append(commAddr);
                commAddr = sb.toString();
                commAddr2Map.put(commAddr, mpedId);
            }
            String meterId = rs2.getString("METER_ID");
            if (meterId != null && !meterId.equals("")) {
                meterIdMap.put(commAddr, meterId);
            }
            String userFlag = rs2.getString("USERFLAG");
            userFlagMap.put(mpedIndex, userFlag);
            Map<String, String> meterAttribute = new HashMap<>();
            meterAttribute.put("ADDR" + SEPARATOR + mpedIndex, commAddr);
            meterAttribute.put("STOPBITS" + SEPARATOR + mpedIndex, rs2.getString("STOPBITS"));
            meterAttribute.put("PARITY" + SEPARATOR + mpedIndex, rs2.getString("PARITY"));
            meterAttribute.put("BYTESIZE" + SEPARATOR + mpedIndex, rs2.getString("BYTESIZE"));
            meterAttribute.put("BAUDRATE" + SEPARATOR + mpedIndex, rs2.getString("BAUD"));
            meterAttribute.put("PORT" + SEPARATOR + mpedIndex, rs2.getString("PORT_NO"));
            meterAttribute.put("MPT" + SEPARATOR + mpedIndex, rs2.getString("IMPORTANT"));
            meterAttribute.put("MPID" + SEPARATOR + mpedIndex, rs2.getString("PROTOCOL_ID"));
            String tgId = rs2.getString("TG_ID");
            if (tgId != null && !tgId.equals("")) {
                meterAttribute.put("TGID" + SEPARATOR + mpedIndex, tgId);
            }
            String lineId = rs2.getString("LINE_ID");
            if (lineId != null && !lineId.equals("")) {
                meterAttribute.put("LINEID" + SEPARATOR + mpedIndex, lineId);
            }
            meterAttributeList.add(meterAttribute);
            // 2016-7-6杭州start 多规约测量点档案缓存MP$**#**
            Map<String, String> meterMPAttribute = new HashMap<>();
            //param 波特率|停止位|无/有校验|偶/奇校验|5-8|位数（无）|报文超时时间单位|报文超时时间
            String param = rs2.getString("BAUD") + "|" + rs2.getString("STOPBITS") + "|"
                    + rs2.getString("CHECK_FLAG") + "|" + rs2.getString("PARITY") + "|"
                    + rs2.getString("BYTESIZE") + "|" + "1" + "|" + rs2.getString("LIMIT_TIME") + "|" + "12000";
            meterMPAttribute.put("MPINDEX" + SEPARATOR + mpedId, rs2.getString("MPED_INDEX"));
            meterMPAttribute.put("ORG" + SEPARATOR + mpedId, rs2.getString("ORG_NO"));
            meterMPAttribute.put("ADDR" + SEPARATOR + mpedId, commAddr);
            meterMPAttribute.put("PTYPE" + SEPARATOR + mpedId, "1");
            meterMPAttribute.put("PTL" + SEPARATOR + mpedId, rs2.getString("PROTOCOL_ID"));
            meterMPAttribute.put("PARAM" + SEPARATOR + mpedId, param);
            meterMPAttribute.put("PORT" + SEPARATOR + mpedId, rs2.getString("PORT_NO"));
            meterMPAttribute.put("TMID" + SEPARATOR + mpedId, rs2.getString("TERMINAL_ID"));
            meterMPAttribute.put("MPT" + SEPARATOR + mpedId, rs2.getString("IMPORTANT"));
            meterMPAttribute.put("DTYPE" + SEPARATOR + mpedId, rs2.getString("METER_DEVICE_TYPE"));
            meterMPList.add(meterMPAttribute);
        }
    }

    /**
     * 获取终端及水汽热测量点信息
     *
     * @throws Exception
     */
    public static List<ObjectBz> getTerminalInfoSQR(JdbcTemplate jdbcTemplate, List<String> list) throws Exception {
        if (list == null || list.size() == 0) { // 没有删除或者没有更新的档案
            return null;
        }
        List<ObjectBz> doclist = new ArrayList<ObjectBz>();
        ObjectBz objBz;

        SqlRowSet rs = jdbcTemplate.queryForRowSet(GET_TMNL_RUN_INFO_SQL, new String[]{list.get(0), list.get(1), list.get(0), list.get(1)});//获取终端信息
        while (rs.next()) {
            objBz = new ObjectBz();
            objBz.setAreaCode(rs.getString("AREA_CODE"));
            objBz.setTermAdd(rs.getString("TERMINAL_ADDR"));
            objBz.setTmnlId(rs.getString("TERMINAL_ID"));
            objBz.setDepartCode(rs.getString("ORG_NO"));
            objBz.setProtocolId(rs.getString("PROTOCOL_ID"));
            logger.info("GET_MPED_INFO_SQL_SQR:" + GET_MPED_INFO_SQL_SQR + " TERMINAL_ID:" + list.get(0));
            SqlRowSet rs4 = jdbcTemplate.queryForRowSet(GET_MPED_INFO_SQL_SQR, new String[]{list.get(0), list.get(1), list.get(1)});//获取终端信息
            Map<String, String> pointNum2Map = new HashMap<String, String>();//P+(测量点序号)
            Map<String, String> commAddr2Map = new HashMap<String, String>();//COMMADDR+电表地址
            Map<String, String> meterIdMap = new HashMap<String, String>();//MI+电表地址   电能表事件通过表地址取meter_id用
            Map<String, String> userFlagMap = new HashMap<String, String>();
            List<Map<String, String>> meterAttributeList = new ArrayList<Map<String, String>>();
            // 2018-4-2杭州start 多规约水汽热测量点档案缓存
            List<Map<String, String>> meterMPList = new ArrayList<Map<String, String>>();
            while (rs4.next()) {
                String mpedIndex = rs4.getString("MPED_INDEX");
                if (StringUtil.isBlank(mpedIndex)) {
                    continue;
                }
                String mpedId = rs4.getString("MPED_ID");
                if (StringUtil.isBlank(mpedId)) {
                    continue;
                }
                pointNum2Map.put(mpedIndex, mpedId);
                String commAddr = rs4.getString(3);
                if (commAddr != null && !commAddr.equals("")) {
                    commAddr2Map.put(commAddr, mpedId);
                }
                String meterId = rs4.getString(16);
                if (meterId != null && !meterId.equals("")) {
                    meterIdMap.put(commAddr, meterId);
                }
                String userFlag = rs4.getString("USERFLAG");
                userFlagMap.put(mpedIndex, userFlag);
                Map<String, String> meterAttribute = new HashMap<String, String>();
                meterAttribute.put("ADDR" + SEPARATOR + mpedIndex, commAddr);
                meterAttribute.put("STOPBITS" + SEPARATOR + mpedIndex, rs4.getString(10));
                meterAttribute.put("PARITY" + SEPARATOR + mpedIndex, rs4.getString(9));
                meterAttribute.put("BYTESIZE" + SEPARATOR + mpedIndex, rs4.getString(8));
                meterAttribute.put("BAUDRATE" + SEPARATOR + mpedIndex, rs4.getString("BAUD"));
                meterAttribute.put("PORT" + SEPARATOR + mpedIndex, rs4.getString("PORT_NO"));
                meterAttribute.put("MPT" + SEPARATOR + mpedIndex, rs4.getString(1));
                meterAttribute.put("MPID" + SEPARATOR + mpedIndex, rs4.getString("PROTOCOL_ID"));
                meterAttributeList.add(meterAttribute);
                // 2016-7-6杭州start 多规约测量点档案缓存MP$**#**
                Map<String, String> meterMPAttribute = new HashMap<String, String>();
                //param 波特率|停止位|无/有校验|偶/奇校验|5-8|位数（无）|报文超时时间单位|报文超时时间
                String param = rs4.getString("BAUD") + "|" + rs4.getString(10) + "|"
                        + rs4.getString(13) + "|" + rs4.getString(9) + "|"
                        + rs4.getString(8) + "|" + "1" + "|" + rs4.getString(14) + "|" + "12000";
                meterMPAttribute.put("MPINDEX" + SEPARATOR + mpedId, rs4.getString("MPED_INDEX"));
                meterMPAttribute.put("ORG" + SEPARATOR + mpedId, rs4.getString("ORG_NO"));
                meterMPAttribute.put("ADDR" + SEPARATOR + mpedId, rs4.getString(3));
                meterMPAttribute.put("PTYPE" + SEPARATOR + mpedId, "1");
                meterMPAttribute.put("PTL" + SEPARATOR + mpedId, rs4.getString("PROTOCOL_ID"));
                meterMPAttribute.put("PARAM" + SEPARATOR + mpedId, param);
                meterMPAttribute.put("PORT" + SEPARATOR + mpedId, rs4.getString("PORT_NO"));
                meterMPAttribute.put("TMID" + SEPARATOR + mpedId, rs4.getString("TERMINAL_ID"));
                meterMPAttribute.put("MPT" + SEPARATOR + mpedId, rs4.getString(1));
                meterMPList.add(meterMPAttribute);
            }
            objBz.setPointNum(pointNum2Map);
            objBz.setCommAddr(commAddr2Map);
            objBz.setMeterIdMap(meterIdMap);
            objBz.setUserFlag(userFlagMap);
            objBz.setMeterAttribute(meterAttributeList);
            objBz.setMeterMP(meterMPList);//MP$+测量点业务标识(mped_id) 2016-7-6杭州start 多规约测量点档案缓存
            Map<String, String> portNum2Map = new HashMap<String, String>();//D+(端口号)
            portNum2Map.put("1", "1");
            objBz.setPortNum(portNum2Map);
            if (pointNum2Map.size() >= 1) {
                doclist.add(objBz);
            }
        }
        return doclist;
    }

    private static String getMpedInfoSql() {
        StringBuffer sb = new StringBuffer();
        sb.append("  SELECT                                 ");
        sb.append("    p.important,                         ");
        sb.append("    p.userflag,                          ");
        sb.append("    CASE                                 ");
        sb.append("      WHEN p.COMM_ADDR IS NULL           ");
        sb.append("      THEN '000000000000'                ");
        sb.append("      ELSE p.COMM_ADDR   ");
        sb.append("    END COMM_ADDR,                       ");
        sb.append("    p.MPED_ID,                           ");
        sb.append("    p.MPED_INDEX,                        ");
        sb.append("    p.protocol_id,                       ");
        sb.append("    p.baud,                              ");
        sb.append("    p.bytesize,                          ");
        sb.append("    p.parity,                            ");
        sb.append("    p.stopbits,                          ");
        sb.append("    p.port_no,                           ");
        sb.append("    p.org_no,                            ");
        sb.append("    p.check_flag,                        ");
        sb.append("    p.limit_time,                        ");
        sb.append("    p.terminal_id,                       ");
        sb.append("    CASE                                 ");
        sb.append("      WHEN p.meter_id IS NULL            ");
        sb.append("      THEN '0'                           ");
        sb.append("      ELSE p.meter_id                    ");
        sb.append("    END meter_id,                        ");
        sb.append("    c.tg_id,                             ");
        sb.append("    c.line_id                            ");
        sb.append("  FROM                                   ");
        sb.append("    r_mped_flow p1                       ");
        sb.append("    JOIN r_tmnl_ir_task rt               ");
        sb.append("      ON p1.shard_no = rt.shard_no       ");
        // 北京 rt.flow_no 河南 rt.app_no
        if ("hn".equalsIgnoreCase(version)) {
            sb.append("      AND p1.flow_no = rt.app_no         ");
        } else if ("bj".equalsIgnoreCase(version)) {
            sb.append("      AND p1.flow_no = rt.flow_no         ");
        }
        sb.append("      AND rt.STATUS = '01'               ");
        sb.append("    RIGHT JOIN r_mped p                  ");
        sb.append("      ON p.meter_id = p1.meter_id        ");
        sb.append("      AND p.shard_no = p1.shard_no       ");
        sb.append("    LEFT JOIN c_mp c                     ");
        sb.append("      ON p.cons_id = c.cons_id           ");
        sb.append("      AND p.shard_no = c.shard_no        ");
        sb.append("  WHERE p.terminal_id = ?                ");
        sb.append("    AND p.shard_no = ?                   ");
        sb.append("     AND p1.mped_id IS NULL              ");
        sb.append("  UNION                                  ");
        sb.append("  ALL                                    ");
        sb.append("  SELECT                                 ");
        sb.append("    p.important,                         ");
        sb.append("    p.userflag,                          ");
        sb.append("    CASE                                 ");
        sb.append("      WHEN p.COMM_ADDR IS NULL           ");
        sb.append("      THEN '000000000000'                ");
        sb.append("      ELSE p.COMM_ADDR  ");
        sb.append("    END COMM_ADDR,                       ");
        sb.append("    p.MPED_ID,                           ");
        sb.append("    p.MPED_INDEX,                        ");
        sb.append("    p.protocol_id,                       ");
        sb.append("    p.baud,                              ");
        sb.append("    p.bytesize,                          ");
        sb.append("    p.parity,                            ");
        sb.append("    p.stopbits,                          ");
        sb.append("    p.port_no,                           ");
        sb.append("    p.org_no,                            ");
        sb.append("    p.check_flag,                        ");
        sb.append("    p.limit_time,                        ");
        sb.append("    p.terminal_id,                       ");
        sb.append("    CASE                                 ");
        sb.append("      WHEN p.meter_id IS NULL            ");
        sb.append("      THEN '0'                           ");
        sb.append("      ELSE p.meter_id                    ");
        sb.append("    END meter_id,                        ");
        sb.append("    c.tg_id,                             ");
        sb.append("    c.line_id                            ");
        sb.append("  FROM                                   ");
        sb.append("    r_mped_flow p                        ");
        sb.append("    JOIN r_tmnl_ir_task rt               ");
        sb.append("      ON p.shard_no = rt.shard_no        ");
        // 北京 rt.flow_no 河南 rt.app_no
        if ("hn".equalsIgnoreCase(version)) {
            sb.append("      AND p.flow_no = rt.app_no          ");
        } else if ("bj".equalsIgnoreCase(version)) {
            sb.append("      AND p.flow_no = rt.flow_no          ");
        }
        sb.append("    LEFT JOIN c_mp c                     ");
        sb.append("      ON p.cons_id = c.cons_id           ");
        sb.append("      AND p.shard_no = c.shard_no        ");
        sb.append("  WHERE rt.STATUS = '01'                 ");
        sb.append("    AND p.FLOW_OPER_TYPE <> '04'         ");
        sb.append("    AND p.terminal_id = ?                ");
        sb.append("     AND p.shard_no = ?                  ");
        return sb.toString();
    }


    private static String getIotMpedInfoSql() {
        StringBuffer sb = new StringBuffer();
        sb.append("  SELECT                                 ");
        sb.append("    p.important,                         ");
        sb.append("    p.userflag,                          ");
        sb.append("    CASE                                 ");
        sb.append("      WHEN p.COMM_ADDR IS NULL           ");
        sb.append("      THEN '000000000000'                ");
        sb.append("      ELSE p.COMM_ADDR   ");
        sb.append("    END COMM_ADDR,                       ");
        sb.append("    p.MPED_ID,                           ");
        sb.append("    p.MPED_INDEX,                        ");
        sb.append("    p.protocol_id,                       ");
        sb.append("    p.baud,                              ");
        sb.append("    p.bytesize,                          ");
        sb.append("    p.parity,                            ");
        sb.append("    p.stopbits,                          ");
        sb.append("    p.port_no,                           ");
        sb.append("    p.org_no,                            ");
        sb.append("    p.check_flag,                        ");
        sb.append("    p.limit_time,                        ");
        sb.append("    p.meter_device_type,                        ");
        sb.append("    p.terminal_id,                       ");
        sb.append("    CASE                                 ");
        sb.append("      WHEN p.meter_id IS NULL            ");
        sb.append("      THEN '0'                           ");
        sb.append("      ELSE p.meter_id                    ");
        sb.append("    END meter_id,                        ");
        sb.append("    c.tg_id,                             ");
        sb.append("    c.line_id                            ");
        sb.append("  FROM                                   ");
        sb.append("    r_mped_flow p1                       ");
        sb.append("    JOIN r_tmnl_ir_task rt               ");
        sb.append("      ON p1.shard_no = rt.shard_no       ");
        // 北京 rt.flow_no 河南 rt.app_no
        if ("hn".equalsIgnoreCase(version)) {
            sb.append("      AND p1.flow_no = rt.app_no         ");
        } else if ("bj".equalsIgnoreCase(version)) {
            sb.append("      AND p1.flow_no = rt.flow_no         ");
        }
        sb.append("      AND rt.STATUS = '01'               ");
        sb.append("    RIGHT JOIN r_mped p                  ");
        sb.append("      ON p.meter_id = p1.meter_id        ");
        sb.append("      AND p.shard_no = p1.shard_no       ");
        sb.append("    LEFT JOIN c_mp c                     ");
        sb.append("      ON p.cons_id = c.cons_id           ");
        sb.append("      AND p.shard_no = c.shard_no        ");
        sb.append("  WHERE p.terminal_id = ?                ");
        sb.append("    AND p.shard_no = ?                   ");
        sb.append("     AND p1.mped_id IS NULL              ");
        sb.append("  UNION                                  ");
        sb.append("  ALL                                    ");
        sb.append("  SELECT                                 ");
        sb.append("    p.important,                         ");
        sb.append("    p.userflag,                          ");
        sb.append("    CASE                                 ");
        sb.append("      WHEN p.COMM_ADDR IS NULL           ");
        sb.append("      THEN '000000000000'                ");
        sb.append("      ELSE p.COMM_ADDR  ");
        sb.append("    END COMM_ADDR,                       ");
        sb.append("    p.MPED_ID,                           ");
        sb.append("    p.MPED_INDEX,                        ");
        sb.append("    p.protocol_id,                       ");
        sb.append("    p.baud,                              ");
        sb.append("    p.bytesize,                          ");
        sb.append("    p.parity,                            ");
        sb.append("    p.stopbits,                          ");
        sb.append("    p.port_no,                           ");
        sb.append("    p.org_no,                            ");
        sb.append("    p.check_flag,                        ");
        sb.append("    p.limit_time,                        ");
        sb.append("    p.meter_device_type,                        ");
        sb.append("    p.terminal_id,                       ");
        sb.append("    CASE                                 ");
        sb.append("      WHEN p.meter_id IS NULL            ");
        sb.append("      THEN '0'                           ");
        sb.append("      ELSE p.meter_id                    ");
        sb.append("    END meter_id,                        ");
        sb.append("    c.tg_id,                             ");
        sb.append("    c.line_id                            ");
        sb.append("  FROM                                   ");
        sb.append("    r_mped_flow p                        ");
        sb.append("    JOIN r_tmnl_ir_task rt               ");
        sb.append("      ON p.shard_no = rt.shard_no        ");
        // 北京 rt.flow_no 河南 rt.app_no
        if ("hn".equalsIgnoreCase(version)) {
            sb.append("      AND p.flow_no = rt.app_no          ");
        } else if ("bj".equalsIgnoreCase(version)) {
            sb.append("      AND p.flow_no = rt.flow_no          ");
        }
        sb.append("    LEFT JOIN c_mp c                     ");
        sb.append("      ON p.cons_id = c.cons_id           ");
        sb.append("      AND p.shard_no = c.shard_no        ");
        sb.append("  WHERE rt.STATUS = '01'                 ");
        sb.append("    AND p.FLOW_OPER_TYPE <> '04'         ");
        sb.append("    AND p.terminal_id = ?                ");
        sb.append("     AND p.shard_no = ?                  ");
        return sb.toString();
    }

    /**
     * @author Dongwei-Chen
     * @Date 2022/7/20 16:57
     * @Description 加载物联网表
     */
    private static String getIotSpMpedInfoSql() {
        StringBuilder sb = new StringBuilder();
        sb.append(" SELECT");
        sb.append(" p.important,");
        sb.append("         p.userflag,");
        sb.append("         p.COMM_ADDR,");
        sb.append("         p.MPED_ID,");
        sb.append("         p.MPED_INDEX,");
        sb.append("         p.protocol_id,");
        sb.append("         p.baud,");
        sb.append("         p.bytesize,");
        sb.append("         p.parity,");
        sb.append("         p.stopbits,");
        sb.append("         p.port_no,");
        sb.append("         p.org_no,");
        sb.append("         p.check_flag,");
        sb.append("         p.limit_time,");
        sb.append("         p.METER_DEVICE_TYPE,");
        sb.append("         p.terminal_id,");
        sb.append("         CASE");
        sb.append(" WHEN p.meter_id IS NULL THEN");
        sb.append(" '0' ELSE p.meter_id");
        sb.append(" END meter_id,");
        sb.append(" c.tg_id,");
        sb.append("         c.line_id");
        sb.append(" FROM");
        sb.append(" r_mped_module p");
        sb.append(" LEFT JOIN c_mp c ON p.cons_id = c.cons_id");
        sb.append(" AND p.shard_no = c.shard_no");
        sb.append(" WHERE");
        sb.append(" p.terminal_id = ?");
        sb.append(" AND p.shard_no = ?");
        return sb.toString();
    }

    private static String getMpedInfoSqlSqr() {
        StringBuffer sb = new StringBuffer();
        sb.append(" SELECT 				                  ");
        sb.append(" case when A.IMPORTANT is null then '0' else A.IMPORTANT end IMPORTANT, ");
        sb.append("        A.USERFLAG,                   ");
        sb.append("        case when D.COMM_ADDR is null then '000000000000' else lpad(D.COMM_ADDR,12,'0') end  COMM_ADDR,      ");
        sb.append("        A.MPED_ID,                    ");
        sb.append("        A.MPED_INDEX,                 ");
        sb.append("        D.PROTOCOL_ID AS PROTOCOL_ID, ");
        sb.append("        D.BAUD        AS BAUD,        ");
        sb.append("        3             BYTESIZE,       ");
        sb.append("        1             PARITY,         ");
        sb.append("        0             STOPBITS,       ");
        sb.append("        D.PORT_NO     AS PORT_NO,     ");
        sb.append("        A.ORG_NO,                     ");
        sb.append("        0             CHECK_FLAG,     ");
        sb.append("        20            LIMIT_TIME,     ");
        sb.append("        A.TERMINAL_ID,                ");
        sb.append("     CASE                             ");
        sb.append(" WHEN A.METER_ID IS NULL THEN         ");
        sb.append("     '0'  			                 ");
        sb.append(" ELSE                                 ");
        sb.append("     A.METER_ID         				 ");
        sb.append(" END METER_ID                         ");
//		sb.append("        A.METER_ID                    ");
        sb.append("   FROM BP_R_MPED A                   ");
        sb.append("   JOIN BP_R_PARA_METER D             ");
        sb.append("     ON A.METER_ID = D.METER_ID       ");
        sb.append("  WHERE A.TERMINAL_ID = ?             ");
        sb.append("    AND A.SHARD_NO = ?                ");
        sb.append("    AND D.SHARD_NO = ?                ");
        return sb.toString();
    }

    private static String getTmnlRunInfoSql() {
        StringBuffer sb = new StringBuffer();
        sb.append(" SELECT                                                              ");
        sb.append("   DISTINCT                                                          ");
        sb.append("   R.ORG_NO,                                                         ");
        sb.append("   R.TERMINAL_ID,                                                    ");
        sb.append("   R.TERMINAL_ADDR,                                                  ");
        sb.append("   R.AREA_CODE,                                                      ");
        sb.append("   R.PROTOCOL_ID,                                                    ");
        sb.append("   R.SHARD_NO,                                                       ");
        sb.append("   case when R.SCHEME_NO is null then '0' else R.SCHEME_NO end SCHEME_NO");
        sb.append(" FROM                                                                ");
        sb.append("   R_TMNL_INFO R                                                     ");
        sb.append("   LEFT JOIN                                                         ");
        sb.append("     (SELECT                                                         ");
        sb.append("       R1.TERMINAL_ID,R1.SHARD_NO                                                          ");
        sb.append("     FROM                                                            ");
        sb.append("       R_TMNL_INFO_FLOW R1                                           ");
        sb.append("       JOIN r_tmnl_ir_task RT                                        ");
        sb.append("         ON R1.SHARD_NO = RT.SHARD_NO                                ");
        // 北京 rt.flow_no 河南 rt.app_no
        if ("hn".equalsIgnoreCase(version)) {
            sb.append("         AND R1.FLOW_NO = RT.app_no                                  ");
        } else if ("bj".equalsIgnoreCase(version)) {
            sb.append("         AND R1.FLOW_NO = RT.FLOW_NO                                  ");
        }
        sb.append("     WHERE RT.STATUS = '01') AA                                      ");
        sb.append("     ON R.SHARD_NO = AA.SHARD_NO AND R.TERMINAL_ID = AA.TERMINAL_ID  ");
        sb.append(" WHERE AA.TERMINAL_ID IS NULL                                        ");
        sb.append("   AND R.TERMINAL_ID = ?                                             ");
        sb.append("   AND R.SHARD_NO = ?                                                ");
        sb.append(" UNION                                                               ");
        sb.append(" ALL                                                                 ");
        sb.append(" SELECT                                                              ");
        sb.append("   DISTINCT                                                          ");
        sb.append("   R.ORG_NO,                                                         ");
        sb.append("   R.TERMINAL_ID,                                                    ");
        sb.append("   R.TERMINAL_ADDR,                                                  ");
        sb.append("   R.AREA_CODE,                                                      ");
        sb.append("   R.PROTOCOL_ID,                                                    ");
        sb.append("   R.SHARD_NO,                                                       ");
        sb.append("   case when R.SCHEME_NO is null then '0' else R.SCHEME_NO end SCHEME_NO ");
        sb.append(" FROM                                                                ");
        sb.append("   R_TMNL_INFO_FLOW R                                                ");
        sb.append("    JOIN r_tmnl_ir_task RT                                           ");
        // 北京 rt.flow_no 河南 rt.app_no
        if ("hn".equalsIgnoreCase(version)) {
            sb.append("     ON R.SHARD_NO = RT.SHARD_NO AND R.FLOW_NO = RT.app_no           ");
        } else if ("bj".equalsIgnoreCase(version)) {
            sb.append("     ON R.SHARD_NO = RT.SHARD_NO AND R.FLOW_NO = RT.FLOW_NO           ");
        }
        sb.append(" WHERE RT.STATUS = '01'                                              ");
        sb.append("   AND R.TERMINAL_ID = ?                                             ");
        sb.append("   AND R.SHARD_NO = ?                                  ");
        return sb.toString();
    }

    public static String getOtsInfoSql(String readFly) {
        StringBuffer sb = new StringBuffer();
        sb.append(" SELECT                                                                           ");
        sb.append("   bb.mped_id,                                                                    ");
        sb.append("   ROUND(                                                                         ");
        sb.append("     24 * IFNULL(c1.extend_content01, 220) * IFNULL(c2.extend_content02, 50) * (  ");
        sb.append("       CASE                                                                       ");
        sb.append("         WHEN bb.WIRING_MODE = '1'                                                ");
        sb.append("         THEN 1                                                                   ");
        sb.append("         ELSE 3                                                                   ");
        sb.append("       END                                                                        ");
        sb.append("     ) / 1000,                                                                    ");
        sb.append("     4                                                                            ");
        sb.append("   ) thr_fz,                                                                      ");
        sb.append("   CASE                                                                           ");
        sb.append("     WHEN bb.MULTIRATE_FALG = '1'                                                 ");
        sb.append("     THEN 1                                                                       ");
        sb.append("     ELSE 0                                                                       ");
        sb.append("   END multirate_falg,                                                            ");
        sb.append("   bb.TERMINAL_ID,                                                                ");
        sb.append("   CASE                                                                           ");
        sb.append("     WHEN bb.para_status = '0'                                                    ");
        sb.append("     OR bb.para_status IS NULL                                                    ");
        sb.append("     THEN '0'                                                                     ");
        sb.append("     ELSE '1'                                                                     ");
        sb.append("   END para_status,                                                               ");
        sb.append("   bb.debug_time                                                                  ");
        sb.append(" FROM                                                                             ");
        sb.append("   (SELECT                                                                        ");
        sb.append("     a.mped_id,                                                                   ");
        sb.append("     a.terminal_id,                                                               ");
        sb.append("     a.shard_no,                                                                  ");
        sb.append("     CASE                                                                         ");
        sb.append("       WHEN a.t_factor IS NULL                                                    ");
        sb.append("       OR a.t_factor = 0                                                          ");
        sb.append("       THEN 1                                                                     ");
        sb.append("       ELSE a.t_factor                                                            ");
        sb.append("     END T_FACTOR,                                                                ");
        sb.append("     a.debug_time,                                                                ");
        sb.append("     a.PARA_STATUS,                                                               ");
        sb.append("     a.rate_num,                                                                  ");
        sb.append("     a.USERFLAG,                                                                  ");
        sb.append("     b.volt_code,                                                                 ");
        sb.append("     b.rated_current,                                                             ");
        sb.append("     b.WIRING_MODE,                                                               ");
        sb.append("     b.MULTIRATE_FALG                                                             ");
        sb.append("   FROM                                                                           ");
        sb.append("      r_mped_flow a1                                                              ");
        sb.append("         JOIN r_tmnl_ir_task rt                                                   ");
        // 北京 rt.flow_no 河南 rt.app_no
        if ("hn".equalsIgnoreCase(version)) {
            sb.append("           ON a1.flow_no = rt.app_no                                              ");
        } else if ("bj".equalsIgnoreCase(version)) {
            sb.append("           ON a1.flow_no = rt.flow_no                                              ");
        }
        sb.append("       AND a1.shard_no = rt.shard_no                                              ");
        sb.append("         AND rt.status = '01'                                                     ");
        sb.append("         RIGHT JOIN                                                               ");
        sb.append("     r_mped a                                                                     ");
        sb.append("       ON a.meter_id = a1.meter_id                                                ");
        sb.append("       AND a.shard_no = a1.shard_no                                               ");
        sb.append("     LEFT JOIN d_meter b                                                          ");
        sb.append("       ON a.meter_id = b.meter_id                                                 ");
        sb.append("       AND a.shard_no = b.shard_no                                                ");
        sb.append("   WHERE a1.meter_id IS NULL                                                      ");
        sb.append(" 		AND a.terminal_id = ?                                                    ");
        sb.append("     AND a.shard_no = ?                                                           ");
        sb.append("   UNION                                                                          ");
        sb.append("   SELECT                                                                         ");
        sb.append("     a.mped_id,                                                                   ");
        sb.append("     a.terminal_id,                                                               ");
        sb.append("     a.shard_no,                                                                  ");
        sb.append("     CASE                                                                         ");
        sb.append("       WHEN a.t_factor IS NULL                                                    ");
        sb.append("       OR a.t_factor = 0                                                          ");
        sb.append("       THEN 1                                                                     ");
        sb.append("       ELSE a.t_factor                                                            ");
        sb.append("     END T_FACTOR,                                                                ");
        sb.append("     a.debug_time,                                                                ");
        sb.append("     a.PARA_STATUS,                                                               ");
        sb.append("     a.rate_num,                                                                  ");
        sb.append("     a.USERFLAG,                                                                  ");
        sb.append("     b.volt_code,                                                                 ");
        sb.append("     b.rated_current,                                                             ");
        sb.append("     b.WIRING_MODE,                                                               ");
        sb.append("     b.MULTIRATE_FALG                                                             ");
        sb.append("   FROM                                                                           ");
        sb.append("     r_mped_flow a                                                                ");
        sb.append("     JOIN r_tmnl_ir_task rt                                                       ");
        // 北京 rt.flow_no 河南 rt.app_no
        if ("hn".equalsIgnoreCase(version)) {
            sb.append("       ON a.flow_no = rt.app_no                                                   ");
        } else if ("bj".equalsIgnoreCase(version)) {
            sb.append("       ON a.flow_no = rt.flow_no                                                   ");
        }
        sb.append("       AND a.shard_no = rt.shard_no                                               ");
        sb.append("     LEFT JOIN d_meter_flow b                                                     ");
        sb.append("       ON a.meter_id = b.meter_id                                                 ");
        sb.append("       AND a.shard_no = b.shard_no                                                ");
        sb.append("       AND a.flow_no = b.flow_no                                                  ");
        sb.append("   WHERE a.FLOW_OPER_TYPE <> '04'                                                 ");
        sb.append("     AND rt.status = '01'                                                         ");
        sb.append(" 		AND a.terminal_id = ?                                                    ");
        sb.append("     AND a.shard_no = ?) bb                                                       ");
        sb.append("   LEFT JOIN P_STANDARD_CODE c1                                                   ");
        sb.append("     ON bb.volt_code = c1.code_value                                              ");
        sb.append("     AND c1.code_type_code = ' meterVolt '                                        ");
        sb.append("   LEFT JOIN P_STANDARD_CODE c2                                                   ");
        sb.append("     ON bb.rated_current = c2.code_value                                          ");
        sb.append("     AND c2.code_type_code = 'meterRcSort'                                        ");


        logger.info("GET_OTS_INFO_SQL:" + sb);
        return sb.toString();
    }

    public static void main(String[] args) {
        String commAddr = "00015057708";
        if (commAddr != null && !"".equals(commAddr)) {
            //FIXME 2022-04-22 光伏模拟档案加载  12位以下补零12位以上不补零
            int length = commAddr.length();
            StringBuilder sb = new StringBuilder();
            if (length < 12) {
                for (int i = 0; i < 12 - length; i++) {
                    sb.append("0");
                }
            }
            sb.append(commAddr);
            commAddr = sb.toString();
            System.out.println(commAddr);
        }
    }
}
