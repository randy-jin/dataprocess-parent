package com.tl.dataprocess;

import com.alibaba.fastjson.JSON;
import com.tl.archives.TerminalArchivesObject;
import com.tl.common.seda.handler.SourceHandler;
import com.tl.common.seda.handler.TaskResult;
import com.tl.common.seda.task.AutoMatch;
import com.tl.datahub.DataHubShardCache;
import com.tl.dataprocess.ConversionUtil.IConversionUtil;
import com.tl.dataprocess.utils.CommonMethod;
//import com.tl.dataprocess.utils.DataConversion;
import com.tl.dataprocess.utils.DataObject;
import com.tl.utils.DataRecords;
import com.tl.utils.DateUtil;
import com.tl.utils.helper.StandaloneRedisHelper;
import com.tl.archives.TerminalArchives;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * Created by huangchunhuai on 2021/11/23.
 */
public class GetDataRecordsHandler extends SourceHandler{

    private  Logger logger = LoggerFactory.getLogger(GetDataRecordsHandler.class);
    @AutoMatch
    TerminalArchives terminalArchives;
    @AutoMatch
    private DataHubShardCache dataHubShardCache;
    @Override
    public void process(Map<String, Object> map, List<TaskResult> list) throws Exception {

        List<String> redisList = StandaloneRedisHelper.lpopPipline(Integer.valueOf(map.get("piplineCount").toString()),map.get("queueName").toString());
        if(redisList==null||redisList.size()==0){
            Thread.sleep(1000);
            return;
        }
        List<DataObject> dataObjectList=new ArrayList<>();
        for (String str:redisList) {
            if(str==null)continue;

            DataRecords dataRecords;
            try{
                dataRecords= JSON.parseObject(str,DataRecords.class);
            }catch (Exception e){
                logger.error("数据转换异常：",e);
                continue;
            }

            String areaCode=dataRecords.getAreaCode();
            String terminalAddr=dataRecords.getTerminalAddr();

            Map<String ,List<Object>> dataMap=dataRecords.getDatas();
            if(dataMap==null){
                continue;
            }
            for (String mapKey:dataMap.keySet()) {
                TerminalArchivesObject terminalArchivesObject=terminalArchives.getMabouut(areaCode,terminalAddr,mapKey,true);
                if(terminalArchivesObject==null){
                    logger.error("无法从缓存获取正确的档案信息:"+areaCode+"_"+terminalAddr+"_"+mapKey);
                    continue;
                }
                terminalArchivesObject.setAreaCode(areaCode);
                terminalArchivesObject.setTerminalAddr(terminalAddr);
                List<Object> fDataList =dataMap.get(mapKey);
                if(fDataList==null||fDataList.size()==0){continue;}

                String meterProtocolId;
                String busiDataItemId=null;
                //dataSrc 数据来源
                //0 自动预抄日冻结  10 自动预抄实时  20 自动直抄日冻结 30 自动直抄实时
                //4 前台手工预抄日冻结  14 前台手工预抄实时  24 前台手工直抄日冻结 34 前台手工直抄实时
                String dataSrc="0";

                //最终数据list，和sqlmapping中fileds里面的顺序一致。
                List<List<Object>> finallyDateList= new ArrayList<>();
                try{
//                    Class clazz = DataConversion.class;
//                    String methodName = dataRecords.getTableName().toLowerCase();
                    String className = dataRecords.getTableName().toLowerCase();
                    Class<?> clazz = Class.forName("com.tl.dataprocess.ConversionUtil.impl." + className);
                    IConversionUtil conversionUtil = (IConversionUtil) clazz.newInstance();
//                    Method md = clazz.getMethod(methodName, List.class, TerminalArchivesObject.class,String.class);
                    if(fDataList.get(0) instanceof List){
                        //未避免引用传递时改变内存中list数据，进行一次备份
                        List<Object> elist=CommonMethod.depCopy(fDataList);
                        for (int i=0;i<fDataList.size();i++) {
                            List<Object> dateList= (List<Object>) fDataList.get(i);
                            List<Object> copyList=(List<Object>) elist.get(i);
                            if(dateList.size()!=copyList.size()){
                                List tempList=new ArrayList<>();
                                for (Object obj:copyList) {
                                    tempList.add(obj);
                                }
                                dateList=tempList;
                            }
                            meterProtocolId=dateList.remove(0).toString();//删除并取出list当前index为0的值作为电表规约
                            busiDataItemId=dateList.remove(0).toString();//删除并取出list当前index为0的值作为业务数据项id
                            dataSrc=dateList.remove(0).toString();//删除并取出list当前index为0的值作为数据来源
                            try{
                                //注：新增方法名跟表名一样
                                finallyDateList.add((List<Object>) conversionUtil.dataProcess(dateList,terminalArchivesObject,dataSrc));
                            }catch (Exception e){
                                logger.error("数据转换异常："+dataRecords.getTableName()+" ：",e);
                                continue;
                            }
                        }
                    }else{
                        meterProtocolId=fDataList.remove(0).toString();//删除并取出list当前index为0的值作为电表规约
                        busiDataItemId=fDataList.remove(0).toString();//删除并取出list当前index为0的值作为业务数据项id
                        dataSrc=fDataList.remove(0).toString();//删除并取出list当前index为0的值作为数据来源
                        try{
                            finallyDateList.add((List<Object>) conversionUtil.dataProcess(fDataList,terminalArchivesObject,dataSrc));
                        }catch (Exception e){
                            logger.error("数据转换异常："+dataRecords.getTableName()+" ：",e);
                            continue;
                        }
                    }
                    //无正确入库格式数据
                    if(finallyDateList==null||finallyDateList.size()==0){
                        continue;
                    }
                    for(List<Object> endlist:finallyDateList){
                        String shardIndex=endlist.remove(0).toString();
                        String clearDate=endlist.remove(0).toString();
                        if(!clearDate.equals("00000000000000")){
                            try{
                                Date date= DateUtil.parse(clearDate);
                                clearDate= DateUtil.formatDate(date,7);
                            }catch (Exception datee){
                                logger.error("缓存日期转换异常："+clearDate+"_"+busiDataItemId+" ：",datee);
                                clearDate="00000000000000";
                            }

                        }
                        //数组用于任务缓存清理
                        Object[] refreshKey = new Object[5];
                        refreshKey[0] = terminalArchivesObject.getTerminalId();
                        refreshKey[1] = shardIndex;
                        refreshKey[2] = clearDate; // 数据召测时间
                        refreshKey[3] = busiDataItemId;
                        refreshKey[4] = dataRecords.getProtocolId();
                        try{
                            if(dataSrc.endsWith("4")){//手工不需要清理缓存
                                refreshKey=null;
                            }
                            //组装dataObject对象
                            CommonMethod.putToDataHub(busiDataItemId,shardIndex,endlist,refreshKey,dataObjectList,dataHubShardCache);
                        }catch (Exception e1){
                            logger.error("dataObject对象生成异常：",e1);
                        }
                    }
                }catch (Exception otherE){
                    logger.error("thie "+dataRecords.getTableName().toLowerCase()+" method error",otherE);
                    continue;
                }
            }
        }
        if(dataObjectList==null||dataObjectList.size()==0){return;}
        list.add(new TaskResult("to_dataHub",dataObjectList));
    }
}
