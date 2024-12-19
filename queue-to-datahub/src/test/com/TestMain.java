package com;

import com.tl.utils.DataRecords;
import org.junit.Test;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.StringRedisSerializer;

import java.util.*;

/**
 * @author wangjunjie
 * @date 2021/11/26 16:13
 */
public class TestMain {

    @Test
    public void test() {
        List baseInfoList = new ArrayList();
        String comm = String.valueOf(baseInfoList.get(1));
    }

    public static void main(String[] args) {
        RedisStandaloneConfiguration rsc = new RedisStandaloneConfiguration();
        rsc.setPort(20024);
        rsc.setPassword("1qaz2wsx4rfv");
        rsc.setHostName("120.55.242.68");

        JedisConnectionFactory jcf = new JedisConnectionFactory(rsc);
        jcf.afterPropertiesSet();
        RedisTemplate<String, String> template = new RedisTemplate<>();
        template.setConnectionFactory(jcf);
        template.setDefaultSerializer(new StringRedisSerializer());
        template.afterPropertiesSet();

        int count = 0;
        DataRecords dr=new DataRecords();
        dr.setTerminalId("8000000020458029"); //
        dr.setAreaCode("1203");
        dr.setCommAddr("000000000000");
        dr.setTerminalAddr("11");
//        while (true) {
//            String json = JSON.toJSONString(e_mp_cur_curve_ud(dr));
//            template.opsForList().leftPush("Q_DATA_TEST", json);
//            System.out.println("写入队列 Q_DATA_TEST 第" + (count++) + "条");
//            if (count == 1000)break;
////            try {
////                Thread.sleep(5000);
////            } catch (InterruptedException e) {
////                e.printStackTrace();
////            }
//        }
        String s = "hello";
        String ss = "hello2";
        String sss = s+"2";
        System.out.println(ss==sss);
    }

    /**
     * 状态字3 状态字1
     * @author wjj
     * @date 2021/11/29 9:03
     * @return
     */
    private static DataRecords e_meter_run_status_num3(DataRecords dr) {
        //e_meter_run_status_num1 70001
        // e_meter_run_status_num3 70003,04000503
        dr.setTableName("E_METER_RUN_STATUS_NUM1");
        Map<String, List<Object>> m=new HashMap<>();
        List<Object> list=new ArrayList<>();
        list.add(8); //protocolId
        list.add("70001"); //busiDataItemId
        list.add("30"); //dataSrc

        list.add(null); //mped_id
        list.add("20211127"); //data_date
        list.add(null); //org_no
        list.add("0000000000001000"); //status
        list.add(null); //shard_no
        list.add(System.currentTimeMillis()); //insert_time
        m.put("000000000000",list);
        dr.setDatas(m);
        return dr;
    }
    
    /**
     * A相电流
     * @author wjj
     * @date 2021/11/29 9:05
     * @return
     */
    private static DataRecords e_edc_cons_cur(DataRecords dr) {
        //e_edc_cons_cur 70301,70302,70303,70304,70305
        // E_EDC_CONS_VOLT 70201,70202,70203,70204
        dr.setTableName("E_EDC_CONS_VOLT");
        Map<String, List<Object>> m=new HashMap<>();
        List<Object> list=new ArrayList<>();
        list.add(8); //protocolId
        list.add("70201"); //busiDataItemId
        list.add("30"); //dataSrc

//        ID:BIGINT,DATA_DATE:STRING,PHASE_FLAG:STRING,ORG_NO:STRING,
// I_VALUE:DOUBLE,POINT_TIME:TIMESTAMP,STATUS:STRING,DATA_SRC:STRING,
// INSERT_TIME:TIMESTAMP,SHARD_NO:STRING
        list.add(null); //mped_id
        list.add("20211127"); //data_date
        list.add("1"); //PHASE_FLAG
        list.add(null); //org_no
        list.add(22.02); //I_VALUE
        list.add(1638067230148L); //POINT_TIME
        list.add("0000000000001000"); //status
        list.add(System.currentTimeMillis()); //insert_time
        list.add(null); //shard_no
        m.put("000000000000",list);
        dr.setDatas(m);
        return dr;
    }

    private static DataRecords e_meter_inside_battery_voltage(DataRecords dr) {
        // e_meter_inside_battery_voltage 60105
        // E_MPED_REAL_OVERDRAFT 00900201
        dr.setTableName("E_MPED_REAL_OVERDRAFT");
        Map<String, List<Object>> m=new HashMap<>();
        List<Object> list=new ArrayList<>();
        list.add(8); //protocolId
        list.add("00900201"); //busiDataItemId
        list.add("30"); //dataSrc
//        MPED_ID:BIGINT,DATA_DATE:STRING,ORG_NO:STRING,BATTERY_VOLTAGE:DOUBLE,
// SHARD_NO:STRING,INSERT_TIME:TIMESTAMP
        list.add(null); //mped_id
        list.add(null); //data_date
        list.add(null); //ORG_NO
        list.add(22.02); //BATTERY_VOLTAGE
        list.add(null); //shard_no
        list.add(System.currentTimeMillis()); //insert_time
        m.put("000000000000",list);
        dr.setDatas(m);
        return dr;
    }

    private static DataRecords e_meter_inside_work_time(DataRecords dr) {
        dr.setTableName("E_METER_INSIDE_WORK_TIME");
        Map<String, List<Object>> m=new HashMap<>();
        List<Object> list=new ArrayList<>();
        list.add(8); //protocolId
        list.add("60106"); //busiDataItemId
        list.add("30"); //dataSrc
//        MPED_ID:BIGINT,DATA_DATE:STRING,ORG_NO:STRING,WORK_TIME:BIGINT,
// SHARD_NO:STRING,INSERT_TIME:TIMESTAMP
        list.add(null); //mped_id
        list.add(null); //data_date
        list.add(null); //ORG_NO
        list.add(440781); //WORK_TIME
        list.add(null); //shard_no
        list.add(System.currentTimeMillis()); //insert_time
        m.put("000000000000",list);
        dr.setDatas(m);
        return dr;
    }

    private static DataRecords e_edc_cons_power(DataRecords dr) {
        dr.setTableName("E_EDC_CONS_FACTOR");
        Map<String, List<Object>> m=new HashMap<>();
        List<Object> list=new ArrayList<>();
        list.add(8); //protocolId
        list.add("71401"); //busiDataItemId
        list.add("30"); //dataSrc
//        ID:BIGINT,DATA_DATE:STRING,DATA_TYPE:STRING,ORG_NO:STRING,P_VALUE:DOUBLE,
// POINT_TIME:TIMESTAMP,STATUS:STRING,DATA_SRC:STRING,INSERT_TIME:TIMESTAMP,SHARD_NO:STRING
        list.add(null); //mped_id
        list.add(null); //data_date
        list.add("0"); //DATA_TYPE
        list.add(null); //ORG_NO
        list.add(0.943); //P_VALUE
        list.add(new Date()); //POINT_TIME
        list.add(null); //STATUS
        list.add(null); //DATA_SRC
        list.add(System.currentTimeMillis()); //insert_time
        list.add(null); //SHARD_NO
        m.put("000000000000",list);
        dr.setDatas(m);
        return dr;
    }

    private static DataRecords e_tmnl_real_time(DataRecords dr) {
        dr.setTableName("e_tmnl_real_time");
        Map<String, List<Object>> m=new HashMap<>();
        List<Object> list=new ArrayList<>();
        list.add(8); //protocolId
        list.add("204016"); //busiDataItemId
        list.add("30"); //dataSrc
//TERMINAL_ID:BIGINT,AREA_CODE:BIGINT,TERMINAL_ADDR:STRING,T_TIME:TIMESTAMP,M_TIME:TIMESTAMP,OVER_TIME:DOUBLE,
        // CON_TIME_NUM:BIGINT,INSERT_TIME:TIMESTAMP,SHARD_NO:STRING
        list.add(null); //TERMINAL_ID
        list.add(null); //AREA_CODE
        list.add(null); //TERMINAL_ADDR
        list.add(System.currentTimeMillis()); //T_TIME
        list.add(System.currentTimeMillis()); //M_TIME
        list.add(0.02); //OVER_TIME
        list.add(3); //CON_TIME_NUM
        list.add(System.currentTimeMillis()); //insert_time
        list.add(null); //SHARD_NO
        m.put("000000000000",list);
        dr.setDatas(m);
        return dr;
    }

    private static DataRecords e_mped_real_buy_record(DataRecords dr) {
        dr.setTableName("E_MPED_REAL_BUY_RECORD");
        Map<String, List<Object>> m=new HashMap<>();
        List<Object> list=new ArrayList<>();
        list.add(8); //protocolId
        list.add("236005"); //busiDataItemId
        list.add("30"); //dataSrc
        //ID:BIGINT,DATA_DATE:STRING,COL_TIME:TIMESTAMP,ORG_NO:STRING,REMAIN_ENEGY:DOUBLE,REMAIN_MONEY:DOUBLE,
        // ALARM_ENEGY:DOUBLE,FAIL_ENEGY:DOUBLE,SUM_ENEGY:DOUBLE,SUM_MONEY:DOUBLE,BUY_NUM:BIGINT,
        // OVERDR_LIMIT:DOUBLE,OVERDR_ENEGY:DOUBLE,INSERT_TIME:TIMESTAMP,SHARD_NO:STRING
        list.add(null); //TERMINAL_ID
        list.add("20211129"); //AREA_CODE
        list.add(System.currentTimeMillis());
        list.add("3");
        list.add(null);
        list.add(298.37);
        list.add(null);
        list.add(null);
        list.add(null);
        list.add(19);
        list.add(null);
        list.add(null);
        list.add(null);
        list.add(null);
        m.put("000000000000",list);
        dr.setDatas(m);
        return dr;
    }

    private static DataRecords e_mp_day_demand(DataRecords dr) {
        dr.setTableName("e_mp_day_demand");
        Map<String, List<Object>> m=new HashMap<>();
        List<Object> list=new ArrayList<>();
        list.add(8); //protocolId
        list.add("205004"); //busiDataItemId
        list.add("30"); //dataSrc
        //ID:BIGINT,DATA_DATE:STRING，DATA_TYPE:STRING,COL_TIME:TIMESTAMP,DEMAND:DOUBLE,DEMAND_TIME:TIMESTAMP,
        // DEMAND1:DOUBLE,DEMAND_TIME1:TIMESTAMP,DEMAND2:DOUBLE,DEMAND_TIME2:TIMESTAMP,DEMAND3:DOUBLE,DEMAND_TIME3:TIMESTAMP,
        // DEMAND4:DOUBLE,DEMAND_TIME4:TIMESTAMP,ORG_NO:STRING,STATUS:STRING,DATA_SRC:STRING,
        // INSERT_TIME:TIMESTAMP,SHARD_NO:STRING,
        list.add(null); //ID
        list.add("20211130"); //DATA_DATE
        list.add("1");//DATA_TYPE
        list.add(1638201600000L);
        list.add(0.5002);
        list.add("2021-11-21 17:19:00");//DEMAND_TIME
        list.add(0.5002);
        list.add("2021-11-21 17:19:00");
        list.add(0.3738);
        list.add("2021-11-28 07:28:00");
        list.add(0.5002);
        list.add("2021-11-21 17:19:00");//DEMAND_TIME3
        list.add(0.3738);
        list.add("2021-11-28 07:28:00");
        list.add(null);
        list.add(null);
        list.add(null);
        list.add(null);
        list.add(null);
        m.put("000000000000",list);
        dr.setDatas(m);
        return dr;
    }

    private static DataRecords e_mp_cur_curve_ud(DataRecords dr) {
        dr.setTableName("e_mp_cur_curve_ud");
        Map<String, List<Object>> m=new HashMap<>();
        List<Object> list=new ArrayList<>();
        list.add(8); //protocolId
        list.add("70306"); //busiDataItemId
        list.add("20"); //dataSrc
        //ID:BIGINT,DATA_DATE:STRING,DATA_TIME:STRING,DATA_POINT_FLAG:BIGINT,PHASE_FLAG:BIGINT,V:DOUBLE,ORG_NO:STRING,
        // INSERT_TIME:TIMESTAMP,DATA_SRC:STRING
        list.add(null); //ID
        list.add("20220110"); //DATA_DATE
        list.add(1638417600000L);
        list.add(1);
        list.add("1");
        List obValue = new ArrayList();
        obValue.add(2.347);
        obValue.add(2.298);
        obValue.add(2.271);
        obValue.add(2.272);
        obValue.add(2.252);
        obValue.add(2.241);
        obValue.add(2.244);
        obValue.add(2.233);
        obValue.add(2.221);
        obValue.add(2.219);
        obValue.add(2.299);
        obValue.add(2.213);
        obValue.add(2.222);
        obValue.add(2.23);
        obValue.add(2.223);
        obValue.add(2.231);
        list.add(obValue);
        list.add(null);
        list.add(null);
        list.add("20");
        m.put("000000000000",list);
        dr.setDatas(m);
        return dr;
    }

    

}
