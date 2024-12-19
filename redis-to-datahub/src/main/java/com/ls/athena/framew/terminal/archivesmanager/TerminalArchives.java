package com.ls.athena.framew.terminal.archivesmanager;

import com.ls.athena.core.ProcessorEvent;
import com.ls.pf.base.api.cache.ICache;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 终端档案
 *
 * @author Administrator
 */
public class TerminalArchives implements ProcessorEvent {

    private static Logger logger = Logger.getLogger(TerminalArchives.class);

    private static final String Ter_Archives = "M$";
    private static final String Ter_MP = "MP$";
    private static final String Ter_Archives_Other = "THM$";
    private static final String ORG = "ORG";
    private static final String TMNLID = "TMNLID";

    private static final String freezeStcpHead = "STCP:100000014564_";

    private ICache archivesremoteRedisCache;
    private ICache archiveslocalCache;
    private int refreshTime = 5;

    private static TerminalArchives terminalArchives = null;

    public TerminalArchives() {
    }

    public TerminalArchives(ICache archivesremoteRedisCache,
                            ICache archiveslocalCache) {
        if (terminalArchives != null) {
            throw new RuntimeException("对象只能构建一次!");
        }
        this.archivesremoteRedisCache = archivesremoteRedisCache;
        this.archiveslocalCache = archiveslocalCache;
        terminalArchives = this;
    }

    public static TerminalArchives getInstance() {
        if (terminalArchives == null) {
            throw new RuntimeException("terminalArchives系统未初始化，或者初始化失败!");
        }
        return terminalArchives;
    }

    /**
     * 面向对象获取前置写入的真实表地址
     *
     * @param areaCode
     * @param tmnlAddr
     * @param meterAddr
     * @return
     */
    @SuppressWarnings("unchecked")
    public String getRealMeterAddrFromLocalOop(String areaCode, String tmnlAddr, String meterAddr) {
        String realAddr = meterAddr;
        String cacheKey = "OOP_ATM#" + areaCode + "_" + tmnlAddr + "_" + meterAddr;
        Object obj = archivesremoteRedisCache.get(cacheKey);
        if (obj != null) {
            realAddr = obj.toString();
        }
        return realAddr;
    }

    @SuppressWarnings("unchecked")
    public Map gettest(String key) {
        Map m = archivesremoteRedisCache.hgetAll(key);
        return m;
    }

    /**
     * 分支箱
     *
     * @param areaCode
     * @param terminalAddr
     * @param key
     * @param value
     */
    @SuppressWarnings("all")
    public void putfzx(String areaCode, int terminalAddr, List datatoredis) {
        String cachekey = "FZX$" + areaCode + "#" + terminalAddr;
        Map<String, String> maps = new HashMap<String, String>();
        maps.put("ZCDB", String.valueOf(datatoredis.get(2)));
        maps.put("SJDB", String.valueOf(datatoredis.get(3)));
        maps.put("ZCBX", String.valueOf(datatoredis.get(4)));
        maps.put("SJBX", String.valueOf(datatoredis.get(5)));
        maps.put("ZCFZX", String.valueOf(datatoredis.get(6)));
        maps.put("SJFZX", String.valueOf(datatoredis.get(7)));
        for (Map.Entry<String, String> obj : maps.entrySet()) {
            archivesremoteRedisCache.hput(cachekey, obj.getKey(), obj.getValue());
        }
    }

    @SuppressWarnings("unchecked")
    public String getCmdForRedisOop(String redisName, String dataitemId) {
        String key = redisName + "_" + dataitemId;
        Object val = archivesremoteRedisCache.get(key);
        return null == val ? null : val.toString();
    }

    /**
     * 查业务数据向缓存
     *
     * @param protocol3761ArchivesObject
     * @param redisName
     * @param dataitemId
     * @return
     */
    @SuppressWarnings("unchecked")
    public String getCmdForRedisOopPublic(Protocol3761ArchivesObject protocol3761ArchivesObject, String redisName, String dataitemId) {
        int protoclId = protocol3761ArchivesObject.getProtocolId();
        String key = redisName + protoclId + "_" + dataitemId;
        Object val = archivesremoteRedisCache.get(key);
        return null == val ? null : val.toString().substring(3);
    }

    /**
     * 获取终端档案
     *
     * @param areaCode
     * @param terminalAddr
     * @return
     */
    @SuppressWarnings("unchecked")
    public TerminalArchivesObject getTmnlArchives(String areaCode, String terminalAddr) {
        String powerUnitNumber;
        String terminalId;
        Map<Object, Object> map = (Map<Object, Object>) archiveslocalCache.get(Ter_Archives + areaCode + "#" + terminalAddr + "#LINE");//M$4105#2779
        if (null == map || map.isEmpty()) {// 本地缓存没有，再从远程取
            List<Object> list = archivesremoteRedisCache.hmget(Ter_Archives + areaCode + "#" + terminalAddr, new String[]{ORG, TMNLID});//一次取多个字段值 hmget ：供电单位编号，终端标识，测量点标识
            if (null == list || list.isEmpty() || null == list.get(0)) {
                return null;
            }
            powerUnitNumber = (String) list.get(0);
            terminalId = (String) list.get(1);
            Map<String, String> mapvalue = new HashMap<String, String>();
            mapvalue.put("ORG", powerUnitNumber);
            mapvalue.put("TMNLID", terminalId);
            archiveslocalCache.put(Ter_Archives + areaCode + "#" + terminalAddr + "#LINE", mapvalue, refreshTime * 60); //那么本地缓存也需要打开  测试：查看本地缓存是否被写入
        } else {
            powerUnitNumber = (String) map.get("ORG");
            terminalId = (String) map.get("TMNLID");
        }
        TerminalArchivesObject terminalArchivesObject = new TerminalArchivesObject(powerUnitNumber, terminalId);
        return terminalArchivesObject;
    }

    /**
     * 获取终端档案和PN/COMMADDR对应的mped_id
     *
     * @param areaCode
     * @param terminalAddr
     * @param pnOrAddr
     * @return
     */
    @SuppressWarnings("unchecked")
    public TerminalArchivesObject getTmnlAndMpedIdArchives(String areaCode, String terminalAddr, String pnOrAddr) {
        String powerUnitNumber;
        String terminalId;
        String id;
        Map<Object, Object> map = (Map<Object, Object>) archiveslocalCache.get(Ter_Archives + areaCode + "#" + terminalAddr + "#" + pnOrAddr);//M$4105#2779
        if (null == map || map.isEmpty()) {// 本地缓存没有，再从远程取
            List<Object> list = archivesremoteRedisCache.hmget(Ter_Archives + areaCode + "#" + terminalAddr, new String[]{ORG, TMNLID, pnOrAddr});//一次取多个字段值 hmget ：供电单位编号，终端标识，测量点标识
            if (null == list || list.isEmpty() || null == list.get(0)) {
                return null;
            }
            powerUnitNumber = (String) list.get(0);
            terminalId = (String) list.get(1);
            if (list.get(2) == null) {
                TerminalArchivesObject terminalArchivesObjectNull = new TerminalArchivesObject(powerUnitNumber, terminalId);
                Map<String, String> mapvalue = new HashMap<String, String>();
                mapvalue.put("ORG", powerUnitNumber);
                mapvalue.put("TMNLID", terminalId);
                archiveslocalCache.put(Ter_Archives + areaCode + "#" + terminalAddr, mapvalue, refreshTime * 60); //那么本地缓存也需要打开  测试：查看本地缓存是否被写入
                return terminalArchivesObjectNull;
            }
            id = (String) list.get(2);
            Map<String, String> mapvalue = new HashMap<String, String>();
            mapvalue.put("ORG", powerUnitNumber);
            mapvalue.put("TMNLID", terminalId);
            mapvalue.put(pnOrAddr, id);
            archiveslocalCache.put(Ter_Archives + areaCode + "#" + terminalAddr + "#" + pnOrAddr, mapvalue, refreshTime * 60); //那么本地缓存也需要打开  测试：查看本地缓存是否被写入
        } else {
            powerUnitNumber = (String) map.get("ORG");
            terminalId = (String) map.get("TMNLID");
            id = (String) map.get(pnOrAddr);
        }
        TerminalArchivesObject terminalArchivesObject = new TerminalArchivesObject(powerUnitNumber, terminalId);
        terminalArchivesObject.setID(id);
        return terminalArchivesObject;
    }

    @SuppressWarnings("unchecked")
    public TerminalArchivesObject getOtherTmnlAndMpedIdArchives(String areaCode, String terminalAddr, String pnOrAddr) {
        String powerUnitNumber;
        String terminalId;
        String id;
        Map<Object, Object> map = (Map<Object, Object>) archiveslocalCache.get(Ter_Archives_Other + areaCode + "#" + terminalAddr + "#" + pnOrAddr);//M$4105#2779
        if (null == map || map.isEmpty()) {// 本地缓存没有，再从远程取
            List<Object> list = archivesremoteRedisCache.hmget(Ter_Archives_Other + areaCode + "#" + terminalAddr, new String[]{ORG, TMNLID, pnOrAddr});//一次取多个字段值 hmget ：供电单位编号，终端标识，测量点标识
            if (null == list || list.isEmpty() || null == list.get(0)) {
                return null;
            }
            powerUnitNumber = (String) list.get(0);
            terminalId = (String) list.get(1);
            if (list.get(2) == null) {
                TerminalArchivesObject terminalArchivesObjectNull = new TerminalArchivesObject(powerUnitNumber, terminalId);
                Map<String, String> mapvalue = new HashMap<String, String>();
                mapvalue.put("ORG", powerUnitNumber);
                mapvalue.put("TMNLID", terminalId);
                archiveslocalCache.put(Ter_Archives_Other + areaCode + "#" + terminalAddr, mapvalue, refreshTime * 60); //那么本地缓存也需要打开  测试：查看本地缓存是否被写入
                return terminalArchivesObjectNull;
            }
            id = (String) list.get(2);
            Map<String, String> mapvalue = new HashMap<String, String>();
            mapvalue.put("ORG", powerUnitNumber);
            mapvalue.put("TMNLID", terminalId);
            mapvalue.put(pnOrAddr, id);
            archiveslocalCache.put(Ter_Archives_Other + areaCode + "#" + terminalAddr + "#" + pnOrAddr, mapvalue, refreshTime * 60); //那么本地缓存也需要打开  测试：查看本地缓存是否被写入
        } else {
            powerUnitNumber = (String) map.get("ORG");
            terminalId = (String) map.get("TMNLID");
            id = (String) map.get(pnOrAddr);
        }
        TerminalArchivesObject terminalArchivesObject = new TerminalArchivesObject(powerUnitNumber, terminalId);
        terminalArchivesObject.setID(id);
        return terminalArchivesObject;
    }

    /**
     * 获取终端档案、mped_id、meter_id
     *
     * @param areaCode
     * @param terminalAddr
     * @param pnOrAddr
     * @param miComm
     * @return
     */
    @SuppressWarnings("unchecked")
    public TerminalArchivesObject getTmnlAndMMidArchives(String areaCode, String terminalAddr, String pnOrAddr, String miComm) {
        String powerUnitNumber;
        String terminalId;
        String id;
        String meterId;
        try {
            Map<Object, Object> map = (Map<Object, Object>) archiveslocalCache.get(Ter_Archives + areaCode + "#" + terminalAddr + "#" + pnOrAddr);//M$4105#2779
            if (null == map || map.isEmpty()) {// 本地缓存没有，再从远程取
                List<Object> list = archivesremoteRedisCache.hmget(Ter_Archives + areaCode + "#" + terminalAddr, new String[]{ORG, TMNLID, pnOrAddr, miComm});//一次取多个字段值 hmget ：供电单位编号，终端标识，测量点标识
                if (null == list || list.isEmpty() || null == list.get(0)) {
                    return null;
                }
                powerUnitNumber = (String) list.get(0);
                terminalId = (String) list.get(1);
                if (null == list.get(2)) {
                    TerminalArchivesObject terminalArchivesObjectNull = new TerminalArchivesObject(powerUnitNumber, terminalId);
                    Map<String, String> mapvalue = new HashMap<String, String>();
                    mapvalue.put("ORG", powerUnitNumber);
                    mapvalue.put("TMNLID", terminalId);
                    archiveslocalCache.put(Ter_Archives + areaCode + "#" + terminalAddr, mapvalue, refreshTime * 60); //那么本地缓存也需要打开  测试：查看本地缓存是否被写入
                    return terminalArchivesObjectNull;
                }
                id = (String) list.get(2);
                if (null == list.get(3)) {
                    TerminalArchivesObject terminalArchivesObjectNull = new TerminalArchivesObject(powerUnitNumber, terminalId);
                    terminalArchivesObjectNull.setID(id);
                    Map<String, String> mapvalue = new HashMap<String, String>();
                    mapvalue.put("ORG", powerUnitNumber);
                    mapvalue.put("TMNLID", terminalId);
                    mapvalue.put(pnOrAddr, id);
                    archiveslocalCache.put(Ter_Archives + areaCode + "#" + terminalAddr + "#" + pnOrAddr, mapvalue, refreshTime * 60); //那么本地缓存也需要打开  测试：查看本地缓存是否被写入
                    return terminalArchivesObjectNull;
                }
                meterId = (String) list.get(3);
                Map<String, String> mapvalue = new HashMap<String, String>();
                mapvalue.put("ORG", powerUnitNumber);
                mapvalue.put("TMNLID", terminalId);
                mapvalue.put(pnOrAddr, id);
                mapvalue.put(miComm, meterId);
                archiveslocalCache.put(Ter_Archives + areaCode + "#" + terminalAddr + "#" + pnOrAddr, mapvalue, refreshTime * 60); //那么本地缓存也需要打开  测试：查看本地缓存是否被写入
            } else {
                powerUnitNumber = (String) map.get("ORG");
                terminalId = (String) map.get("TMNLID");
                id = (String) map.get(pnOrAddr);
                meterId = (String) map.get(miComm);
            }

        } catch (Exception e) {
            return null;
        }
        TerminalArchivesObject terminalArchivesObject = new TerminalArchivesObject(powerUnitNumber, terminalId);
        terminalArchivesObject.setID(id);
        terminalArchivesObject.setMeterId(meterId);
        return terminalArchivesObject;
    }


    public  String getPnFromMpedId(String mpedId){


        return (String) archivesremoteRedisCache.hget(Ter_MP+mpedId,"MPINDEX");
    }

    public void refreshCache(String key) {
        archiveslocalCache.remove(key);
    }


    public void start() throws Exception {
    }

    public void stop() throws Exception {
    }


    public void setRefreshTime(int refreshTime) {
        this.refreshTime = refreshTime;
    }
}
