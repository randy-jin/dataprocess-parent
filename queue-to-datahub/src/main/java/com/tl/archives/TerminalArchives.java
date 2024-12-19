package com.tl.archives;

import com.tl.utils.helper.ClusterRedisHelper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by huangchunhuai on 2021/11/24.
 */
@Component
public class TerminalArchives {

    @Autowired
    ClusterRedisHelper clusterRedisHelper;

    private static final String Ter_Archives = "M$";
    private static final String Ter_Archives_Other = "THM$";
    private static final String ORG = "ORG";
    private static final String TMNLID = "TMNLID";
    private static final String COMMADDR = "COMMADDR";
    private static final String MPEDID = "MPEDID";
    private static final String METERID = "METERID";
    private static final String MI = "MI";



    /**
     * 获取终端信息
     * @param areaCode
     * @param terminalAddr
     * @return
     */
    public TerminalArchivesObject getTmnl(String areaCode, String terminalAddr) {
        List<String> filedList=new ArrayList<>();
        filedList.add(TMNLID);
        filedList.add(ORG);
        Map<String,String> bdMap=ArchivesMapManager.currentMap.get(Ter_Archives + areaCode + "#" + terminalAddr);
        List<String> valueList=new ArrayList<>();
        if (bdMap == null) {
            valueList =  clusterRedisHelper.redisTemplate.opsForHash().multiGet(Ter_Archives + areaCode + "#" + terminalAddr,filedList);
            if (null == valueList || valueList.isEmpty() || null == valueList.get(0)) {
                return null;
            }else{
                bdMap=new HashMap<>();
                bdMap.put(TMNLID,valueList.get(0));
                bdMap.put(ORG,valueList.get(1));
                ArchivesMapManager.currentMap.put(Ter_Archives + areaCode + "#" + terminalAddr,bdMap,ArchivesMapManager.EXPIRY);//本地缓存60s
            }
        }else{
            valueList.add(bdMap.get(TMNLID));
            valueList.add(bdMap.get(ORG));
        }
        TerminalArchivesObject terminalArchivesObject=new TerminalArchivesObject();
        terminalArchivesObject.setTerminalId(valueList.get(0));
        terminalArchivesObject.setPowerUnitNumber(valueList.get(1));
        return terminalArchivesObject;
    }

    /**
     * 获取电表相关信息
     * @param areaCode
     * @param terminalAddr
     * @param pnOrcommaddr
     * @param isMeter
     * @return
     */
    public TerminalArchivesObject getMabouut(String areaCode, String terminalAddr,String pnOrcommaddr,boolean isMeter ){
        List<String> filedList=new ArrayList<>();
        filedList.add(TMNLID);
        filedList.add(ORG);
        if(pnOrcommaddr.startsWith("P")){
            filedList.add(pnOrcommaddr);
        }else{
            filedList.add(COMMADDR+pnOrcommaddr);
            if(isMeter){
                filedList.add(MI+pnOrcommaddr);
            }
        }
        Map<String,String> bdMap=ArchivesMapManager.currentMap.get(Ter_Archives + areaCode + "#" + terminalAddr+"#"+pnOrcommaddr+"#"+isMeter);
        List<String> valueList=new ArrayList<>();
        if (bdMap == null) {
            valueList =  clusterRedisHelper.redisTemplate.opsForHash().multiGet(Ter_Archives + areaCode + "#" + terminalAddr,filedList);
            if (null == valueList || valueList.isEmpty() || null == valueList.get(0)) {
                return null;
            }else{
                bdMap=new HashMap<>();
                bdMap.put(TMNLID,valueList.get(0));
                bdMap.put(ORG,valueList.get(1));
                bdMap.put(MPEDID,valueList.get(2));
                if(isMeter){
                    bdMap.put(METERID,valueList.get(3));
                }
                ArchivesMapManager.currentMap.put(Ter_Archives + areaCode + "#" + terminalAddr+"#"+pnOrcommaddr+"#"+isMeter,bdMap,ArchivesMapManager.EXPIRY);//本地缓存60s
            }
        }else{
            valueList.add(bdMap.get(TMNLID));
            valueList.add(bdMap.get(ORG));
            valueList.add(bdMap.get(MPEDID));
            if(isMeter) valueList.add(bdMap.get(METERID));
        }


        TerminalArchivesObject terminalArchivesObject=new TerminalArchivesObject();
        terminalArchivesObject.setTerminalId(valueList.get(0));
        terminalArchivesObject.setPowerUnitNumber(valueList.get(1));
        terminalArchivesObject.setMpedId(valueList.get(2));
        if(isMeter){
            terminalArchivesObject.setMeterId(valueList.get(3));
        }
        return terminalArchivesObject;
    }
}
