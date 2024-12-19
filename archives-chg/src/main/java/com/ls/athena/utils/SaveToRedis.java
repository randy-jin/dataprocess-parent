package com.ls.athena.utils;

import com.alicloud.openservices.tablestore.SyncClient;
import com.ls.athena.ots.OtsUtil;
import com.ls.pf.common.dataCatch.bz.bo.ObjectBz;
import com.ls.pf.common.dataCath.bz.service.IOperateBzData;
import org.future.hand.HandClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SaveToRedis {

    private final static Logger logger = LoggerFactory.getLogger(SaveToRedis.class);

    public static void save(JdbcTemplate jdbcTemplate, IOperateBzData operateBzData, SyncClient syncClient, ArchivesObject archives, String upSql, String flag, int otsWrite) {
        String terminalId = archives.getTERMINAL_ID();
        String shardNo = archives.getSHARD_NO();
        List<String> saveList = new ArrayList<>();
        saveList.add(terminalId);
        saveList.add(shardNo);
        try {
            //物联网表缓存加载需要调用ArchivesChangeUtils.getIotTerminalInfo(jdbcTemplate, saveList);
            List<ObjectBz> list = ArchivesChangeUtils.getIotTerminalInfo(jdbcTemplate, saveList);
            if (list != null && list.size() != 0 && list.get(0).getPointNum().size() > 0) { // 终端下有测量点档案需要加载
                logger.info("list.size:" + list.size());
                operateBzData.saveObj(list);
                logger.info("save redis success!!!!!");
            } else if (list.size() == 0) {//数据库没有该终端，删除redis档案信息防止主动上报串户
                String addr = (String) operateBzData.getTB(terminalId).get("ADDR");
                if (!"".equals(addr) && null != addr) {
                    String[] addr2 = addr.split("\\|");
                    if (addr2.length == 2) {
                        ObjectBz objBz = new ObjectBz();
                        objBz.setAreaCode(addr2[0]);
                        objBz.setTermAdd(addr2[1]);
                        objBz.setTmnlId(terminalId);
                        operateBzData.removeObj(objBz);
                        logger.info("当前终端数据库没有查询到终端信息，按照拆除处理，terminalId:" + terminalId);
                    } else {
                        logger.info("redis里终端信息不匹配，terminalId:" + terminalId);
                    }
                } else {
                    logger.info("redis获取不到拆除终端信息，terminalId:" + terminalId);
                }
            } else {
                logger.info("no data for add!!!");
            }

            /************水汽热start**************/
            List<ObjectBz> listSQR = ArchivesChangeUtils.getTerminalInfoSQR(jdbcTemplate, saveList);
            if (listSQR != null && listSQR.size() != 0 && listSQR.get(0).getPointNum().size() > 0) { // 没有添加与更新的档案
                logger.info("listSQR.size:" + listSQR.size());
                operateBzData.saveObjSQR(listSQR);
                logger.info("save SQRredis success!!!!!");
            } else if (listSQR.size() == 0) {
                logger.info("no SQR终端data for add!!!");
            } else {
                logger.info("no SQRdata for add!!!");
            }
            /************数据核查档案start**************/
            if (otsWrite == 1) {// 档案数据同步ots打开
                List<Map<String, String>> listOts = ArchivesChangeUtils.getListToOts(jdbcTemplate, saveList);
                for (List<Map<String, String>> mapList : HandClient.splitList(listOts, 200)) {
                    try {
                        logger.info("listOts.size()：" + listOts.size());
                        OtsUtil.putOts(syncClient, "dim_dayread_archives", mapList);
                        logger.info("WRITE OTS SUCCESS!!!!!");
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
            String[] strings;
            if (flag != null) {
                strings = new String[]{flag, saveList.get(0), saveList.get(1)};
                updateProfile(jdbcTemplate, saveList, upSql, strings);//更新r_tmnl_profile表状态为1：成功
            }
        } catch (Exception e) {
            logger.error("Update archives error:", e);
            String[] strings;
            if (flag != null) {
                strings = new String[]{"2", saveList.get(0), saveList.get(1)};
                updateProfile(jdbcTemplate, saveList, upSql, strings);//更新r_tmnl_profile表状态为1：成功
            } else {
                strings = new String[]{saveList.get(0), saveList.get(1)};
                updateProfile(jdbcTemplate, saveList, upSql, strings);//更新r_tmnl_profile表状态为1：成功
            }
        }
    }


    private static void updateProfile(JdbcTemplate jdbcTemplate, List<String> list, String upSql, String[] params) {
        int rs = jdbcTemplate.update(upSql, params);
        String tableName = "r_tmnl_profile";
        if (params.length != 3) {
            tableName = "r_tmnl_profile_all";
        }
        logger.info("rs=" + rs + "  更新" + tableName + "表：terminal_ID=" + list.get(0) + "  shard_no=" + list.get(1) + (params.length != 3 ? "" : " flag=" + params[0]));
    }


}
