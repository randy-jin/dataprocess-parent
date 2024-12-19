package com.tl.dataprocess.rocketMQ.model;

import cn.hutool.core.date.DateUtil;
import cn.hutool.core.util.RandomUtil;
import com.aliyun.datahub.DatahubClient;
import com.aliyun.datahub.DatahubConfiguration;
import com.aliyun.datahub.auth.Account;
import com.aliyun.datahub.auth.AliyunAccount;
import com.aliyun.datahub.common.data.RecordSchema;
import com.aliyun.datahub.exception.AuthorizationFailureException;
import com.aliyun.datahub.exception.DatahubClientException;
import com.aliyun.datahub.exception.InvalidParameterException;
import com.aliyun.datahub.exception.ResourceNotFoundException;
import com.aliyun.datahub.model.PutRecordsResult;
import com.aliyun.datahub.model.RecordEntry;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * @author ：chenguanxing
 * @date ：Created in 2022/7/7 16:03
 * @description ：
 * @version: 1.0.0.0
 */
public class TestWrite2Datahub {

    private static String accessId = null;
    private static String accessKey = null;
    private static String endpoint = null;
    private static String project = "easfz";
    private static String topic = "bt_e_branch_red_source";

    public static void main(String[] args) {
        loadconfig();

        Account account = new AliyunAccount(accessId, accessKey);
        DatahubConfiguration conf = new DatahubConfiguration(account, endpoint);
        //构建一个新的客户端调用DataHub服务方法。
        DatahubClient datahubClient = new DatahubClient(conf);

        String shardId = "0";
        // 获取schema
        RecordSchema recordSchema = datahubClient.getTopic(project, topic).getRecordSchema();

        // 生成十条数据
        List<RecordEntry> recordEntries = new ArrayList<>();
        for (int i = 0; i < 3; ++i) {
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

//            recordEntry.setString("terminal_id","8000000020336034");
//            recordEntry.setTimeStampInDate("event_time",new Date());
//            recordEntry.setTimeStampInDate("restart_time",new Date());


//            eventRecordEntry(recordEntry);
//            volCurveEntry(recordEntry);
            userPowerEntry(recordEntry);

            recordEntry.setShardId(shardId);
            recordEntries.add(recordEntry);
        }
        try {
            // 服务端从2.12版本开始支持，之前版本请使用putRecords接口
//            datahubClient.putRecordsByShard(Constant.projectName, Constant.topicName, shardId, recordEntries);
            PutRecordsResult result = datahubClient.putRecords(project,topic, recordEntries);
//            //得到鼠标指针的操作  参数：项目名，主题名，cursor的Id,光标类型
//            GetCursorResult cursor = datahubClient.getCursor(project, topic, "0", GetCursorRequest.CursorType.OLDEST);
//            //根据表信息获取表结果集 取出1条记录
//            GetRecordsResult r = datahubClient.getRecords(project, topic, "0", cursor.getCursor(), 1, recordSchema);
//            System.out.println(r.getRecords().get(0));
            System.out.println("write data successful");
        }  catch (InvalidParameterException e) {
            System.out.println("invalid parameter, please check your parameter");
            System.exit(1);
        } catch (AuthorizationFailureException e) {
            System.out.println("AK error, please check your accessId and accessKey");
            System.exit(1);
        } catch (ResourceNotFoundException e) {
            System.out.println("project or topic or shard not found");
            System.exit(1);
        } catch (DatahubClientException e) {
            System.out.println("other error");
            System.out.println(e);
            System.exit(1);
        }
    }

    private static void loadconfig() {
        endpoint = "https://dh-cn-hangzhou.aliyuncs.com";
        project = "hntl";
        accessId = "wftF2HEfAtTCx2zr";
        accessKey = "3ZSdbOdkNTQVuU9E2qHP1N4571iGe9";
//        topic = "e_event_erc14_source";
//        topic = "e_mp_vol_curve_ud_source";
//        topic = "e_mp_cur_curve_ud_source";
//        topic = "e_mp_power_curve_ud_source";
//        topic = "e_mp_factor_curve_ud_source";
//        topic = "e_mp_read_curve_ud_source";
        topic = "e_meter_event_no_power_source";

    }

    private static void eventRecordEntry(RecordEntry recordEntry){
        recordEntry.setBigint(0,1000L);
        recordEntry.setBigint(1,8000000020336034L);
        recordEntry.setTimeStampInDate(2,new Date());
        recordEntry.setTimeStampInDate(3,new Date());
        recordEntry.setString(4,null);
        recordEntry.setString(5,null);
        recordEntry.setString(6,null);
        recordEntry.setString(7,null);
        recordEntry.setString(8,null);
        recordEntry.setString(9,null);
        recordEntry.setTimeStampInDate(10,null);
        recordEntry.setString(11,null);
    }

    private static void volCurveEntry(RecordEntry recordEntry){
        recordEntry.setBigint(0,110000002613426L);
        recordEntry.setString(1,"20200808");
        recordEntry.setString(2,"1");
        recordEntry.setBigint(3,10L);
        recordEntry.setBigint(4,1L);
        recordEntry.setDouble(5,13.44D);
        recordEntry.setString(6,"414030326");
        recordEntry.setTimeStampInDate(7,new Date());
        recordEntry.setString(8,null);
    }

    private static void userPowerEntry(RecordEntry recordEntry){
        recordEntry.setBigint(0,123456L);
        recordEntry.setTimeStampInDate(1,new Date());
        recordEntry.setString(2,"4321");
        recordEntry.setString(3, RandomUtil.randomInt(10, 20)>15?"0":"1");
        recordEntry.setTimeStampInDate(4, DateUtil.yesterday());
        recordEntry.setTimeStampInDate(5, RandomUtil.randomInt(10, 20)>15?null:new Date());
        recordEntry.setString(6, "41401");
        recordEntry.setString(7, null);
    }
}
