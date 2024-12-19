package com.tl.subjoin.utils;

import com.alibaba.fastjson.JSON;
import redis.clients.jedis.Jedis;

import java.util.*;

/**
 * Created by huangchunhuai on 2021/10/27.
 */
public class Test {
    public static void main(String[] args) {

        Jedis j=new Jedis("21.32.65.67",28479);
        j.auth("1qaz2wsx4rfv");


        Set<String> keys=j.keys("FEE:PRCCALL:GROUP:TMNL:*");
        for (String key:keys) {
            j.del(key);
        }


//        List<String> list = new ArrayList<>();
//        for (int i = 0; i < 100; i++) {
//            list.add(String.valueOf(i));
//        }
//        Map<Integer, List<String>> map = new HashMap<>();
//        for (int i = 0; i < 100; i++) {
//            int key = Integer.valueOf(list.get(i)) % 48;
//            List<String> subList = map.get(key);
//            if (subList == null) {
//                subList = new ArrayList<>();
//                map.put(key, subList);
//            }
//            subList.add(list.get(i));
//        }
//
//        for (Map.Entry<Integer, List<String>> item : map.entrySet()) {
//            Integer key = item.getKey();
//            List<String> val = item.getValue();
//            System.out.println(key + "：" + val);
//        }

//        ArchivesObject ao=new ArchivesObject();
//        ao.setAPP_NO("fuck");
//        ao.setSHARD_NO("11402");
//        ao.setTERMINAL_ID("8000000020730371");
//
//
//        Jedis j=new Jedis("120.55.242.68",20024);
//        j.auth("1qaz2wsx4rfv");
//
//        j.lpush("Q_CHG_ARCHIVES_SUBJOIN"+"_"+ao.getSHARD_NO(), JSON.toJSONString(ao));



//        1、查询出来所有终端id和shard_no 组装成ArchivesObject对象
//        2、遍历数据list 放到redis队列中。Q_CHG_ARCHIVES_SUBJOIN_[shard_no]
    }
}

