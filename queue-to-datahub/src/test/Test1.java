import com.alibaba.fastjson.JSON;
import com.tl.utils.DataRecords;
import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by huangchunhuai on 2021/11/26.
 */
public class Test1 {
    private static final int count=10;
    public static void main(String[] args) {
        DataRecords dr=new DataRecords();
        dr.setTerminalId("8000000020458029");
        dr.setAreaCode("1203");
        dr.setCommAddr("000000000000");
        dr.setTerminalAddr("11");
        dr.setTableName("e_mp_day_read");
        dr.setProtocolId("1");
        Map<String,List<Object>>  m=new HashMap<>();
        List<Object> endlist=new ArrayList<>();
        for(int m1=0;m1<count;m1++){
            List<Object> list=new ArrayList<>();
            list.add(8);
            list.add("233008");
            list.add("10");
            list.add(null);
            list.add("1");
            list.add("20211126");
            list.add(System.currentTimeMillis());
            list.add(0.0+m1);
            list.add(22.02);
            list.add(11.22);
            list.add(10.02);
            list.add(70.02);
            for(int i=5;i<=14;i++){
                list.add(null);
            }
            endlist.add(list);
        }
        m.put("000000000000",endlist);
        dr.setDatas(m);
        Jedis j=new Jedis("120.55.242.68",20024);
        j.auth("1qaz2wsx4rfv");

        j.lpush("Q_DATA_TEST", JSON.toJSONString(dr));


        j.close();
    }
}
