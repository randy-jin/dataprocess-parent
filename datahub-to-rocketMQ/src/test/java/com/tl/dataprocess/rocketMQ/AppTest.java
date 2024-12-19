package com.tl.dataprocess.rocketMQ;

import cn.hutool.core.date.DateUtil;
import cn.hutool.core.map.MapUtil;
import cn.hutool.core.util.ObjectUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.aliyun.datahub.DatahubClient;
import com.aliyun.datahub.common.data.RecordSchema;
import com.aliyun.datahub.model.RecordEntry;
import com.tl.dataprocess.rocketMQ.manager.RedisCacheManager;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;

import javax.annotation.Resource;
import java.util.*;

import static com.tl.dataprocess.rocketMQ.constant.WriterConstant.*;

/**
 * @author liTongHui
 * @date 2022/4/22 16:46
 * @desc
 **/
@SpringBootTest
public class AppTest {

    @Resource
    private RedisCacheManager redisCacheManager;

    @Resource
    private DatahubClient datahubClient;

    @Value("${datahub.default.project}")
    private String project;

    @Value("${datahub.default.topic.blackoutEvent}")
    private String topic;

//    @Value("${datahub.default.subId}")
//    private String subId;

    @Test
    public void redisTest() throws Exception {
//        redisTemplate.opsForValue().set("test","test");
//        System.out.println("cdhe");
//        System.out.println(redisCacheManager.getRedisTemplate().getValueSerializer());
        Map<String, Object> map =  redisCacheManager.hmget("TB$"+"8000000020336034");
        System.out.println(map);
//        System.out.println(map.get("ORG"));
//        System.out.println(map.get("ADDR"));

        String[] addr = map.get("ADDR").toString().split("\\|");
        System.out.println(addr[0]);
        System.out.println(addr[1]);

        Map<String, Object> map1 = redisCacheManager.hmget("M$"+addr[0]+"#"+addr[1]);
        System.out.println(map1);
        String cedian = "P$"+ addr[0]+"#"+addr[1] +"#";
        for (String key : map1.keySet()) {
            //key
            if(StrUtil.startWith(key, "P")){
                cedian += StrUtil.removePrefix(key, "P");
                break;
            }
        }

        System.out.println(cedian);
        Object cc = redisCacheManager.hget(cedian,"TGID");
        System.out.println(cc.toString());
    }

    @Test
    public void redisTest1() throws Exception{
        Map<String, Object> build = MapUtil.builder(new HashMap<String, Object>())
                .put(EVENT_TIME, new Date())
                .put(TERMINAL_ID, "8000000020336034")
                .put(RESTART_TIME, new Date()).build();

        build = getMsgInRedis(build);
//        System.out.println(build);
        System.out.println(JSONUtil.toJsonPrettyStr(build));
    }

    /**
     * 用tmnlId去查询orgNo等
     * @param fieldValue 注意：key都是大写的
     */
    private Map<String, Object> getMsgInRedis(Map<String, Object> fieldValue){
        Map<String, Object> build = new HashMap<String, Object>();

        build.put(TERMINAL_ID,fieldValue.get(TERMINAL_ID));
        Map<String, Object> mapTB =  redisCacheManager.hmget(TB_KEY + fieldValue.get(TERMINAL_ID));
        build.put(ORG_NO,mapTB.get(ORG));

        String[] addr = mapTB.get(ADDR).toString().split("\\|");
        Map<String, Object> mapM = redisCacheManager.hmget(StrUtil.format(M_KEY,addr[0],addr[1]));

        String point = "";
        for (String key : mapM.keySet()) {
            //key
            if(StrUtil.startWith(key, "P")){
                point = StrUtil.removePrefix(key, "P");
                break;
            }
        }
        if(StrUtil.isBlank(point)){
            throw new RuntimeException("Measurement point is Empty");
        }
        Object tgId = redisCacheManager.hget(StrUtil.format(P_KEY,addr[0],addr[1],point),TGID);
        build.put(TG_ID,tgId);
        build.put(OUTAGE_BEGIN_TIME, DateUtil.format((Date) fieldValue.get(EVENT_TIME),"yyyy-MM-dd hh:mm:ss"));
        build.put(OUTAGE_FLAG,0);
        build.put(OUTAGE_END_TIME,null);
        if(ObjectUtil.isNotNull(fieldValue.get(RESTART_TIME))){
            build.put(OUTAGE_END_TIME,DateUtil.format((Date) fieldValue.get(RESTART_TIME),"yyyy-MM-dd hh:mm:ss"));
            build.put(OUTAGE_FLAG,1);
        }
        build.put(EVENT_TIME,DateUtil.format((Date) fieldValue.get(EVENT_TIME),"yyyy-MM-dd hh:mm:ss"));
        return build;
    }


    @Test
    public void writeToData() throws Exception{
        String shardId = "0";
        // 获取schema
        RecordSchema recordSchema = datahubClient.getTopic(project, topic).getRecordSchema();

        // 生成十条数据
        List<RecordEntry> recordEntries = new ArrayList <>();
        for (int i = 0; i < 5; ++i) {
            // 对每条数据设置额外属性，例如ip 机器名等。可以不设置额外属性，不影响数据写入
            RecordEntry recordEntry = new RecordEntry(recordSchema);
//            Field f1 = new Field(TERMINAL_ID, FieldType.STRING);
//            Field f2 = new Field(EVENT_TIME, FieldType.TIMESTAMP);
//            Field f3 = new Field(RESTART_TIME, FieldType.TIMESTAMP);
//            Field[] fields = new Field[]{f1,f2,f3};
//
//            String v1 = "8000000020336034";
//            Date v2 = new Date();
//            Date v3 = new Date();
//            Object[] values = new Object[]{v1,v2,v3};

            recordEntry.setString(TERMINAL_ID.toLowerCase(),"8000000020336034");
            recordEntry.setTimeStampInDate(EVENT_TIME.toLowerCase(),new Date());
            recordEntry.setTimeStampInDate(RESTART_TIME.toLowerCase(),new Date());

            recordEntry.setShardId(shardId);
        }
        try {
            // 服务端从2.12版本开始支持，之前版本请使用putRecords接口
//            datahubClient.putRecordsByShard(Constant.projectName, Constant.topicName, shardId, recordEntries);
            datahubClient.putRecords(project,topic, recordEntries);

            //得到鼠标指针的操作  参数：项目名，主题名，cursor的Id,光标类型
//            GetCursorResult cursor = datahubClient.getCursor(project, topic, "0", GetCursorRequest.CursorType.OLDEST);
//            //根据表信息获取表结果集 取出1条记录
//            GetRecordsResult r = datahubClient.getRecords(project, topic, "0", cursor.getCursor(), 1, recordSchema);
//            System.out.println(r.getRecords().get(0));
            System.out.println("write data successful");
        } catch (Exception e) {
            System.out.println("invalid parameter, please check your parameter");
            System.exit(1);
        }
    }
}
