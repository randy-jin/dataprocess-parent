package com.tl.archives;

import com.tl.utils.helper.ClusterRedisHelper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * Created by huangchunhuai on 2021/11/26.
 */
@Component
public class DataitemArchives {
    @Autowired
    ClusterRedisHelper clusterRedisHelper;

    private static final String PT = "PT$";
    private static final int  EXPIRY=120*1000;

    public String getBusiDataitemId(String protocolId,int index){
        String busiDataItemId=null;
        String filed=protocolId;
        switch (protocolId){
            case "8":
                if(index>0){
                    filed=filed+"-"+index;
                }
                break;
        }

        Map<String,String> bdiMap=ArchivesMapManager.currentMap.get(PT+protocolId);
        if(bdiMap!=null){
            busiDataItemId=bdiMap.get(filed);
        }
        if(busiDataItemId==null){
            try{
                busiDataItemId= clusterRedisHelper.redisTemplate.boundHashOps(PT+protocolId).get(filed).toString();
            }catch (Exception e){
                return null;
            }
            bdiMap.put(filed,busiDataItemId);
            ArchivesMapManager.currentMap.put(PT+protocolId,bdiMap,EXPIRY);
        }
        return busiDataItemId;
    }
}
