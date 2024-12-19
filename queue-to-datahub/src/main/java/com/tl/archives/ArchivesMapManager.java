package com.tl.archives;

import com.tl.utils.ExpiryMap;

import java.util.Map;

/**
 * Created by huangchunhuai on 2021/11/26.
 */
public class ArchivesMapManager {
    //本地缓存
    public static ExpiryMap<String,Map<String,String>> currentMap=new ExpiryMap<>();
    //有效期
    public static final int  EXPIRY=60*1000;
//
//    private static ArchivesMapManager instance =new ArchivesMapManager();
//    public static ArchivesMapManager getInstance(){
//        return instance;
//    }
//    private ArchivesMapManager (){}

}
