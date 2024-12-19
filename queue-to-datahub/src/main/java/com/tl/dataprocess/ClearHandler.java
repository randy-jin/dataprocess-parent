package com.tl.dataprocess;

import com.google.common.collect.Queues;
import com.tl.common.seda.handler.SourceHandler;
import com.tl.common.seda.handler.TaskHandler;
import com.tl.common.seda.handler.TaskResult;
import com.tl.common.seda.task.AutoMatch;
import com.tl.common.seda.task.Task;
import com.tl.common.seda.task.TaskFailHandler;
import com.tl.utils.PropertiesUtils;
import com.tl.utils.StringUtil;
import com.tl.utils.refreshkey.ClearHelper;
import org.springframework.beans.factory.annotation.Value;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Created by huangchunhuai on 2021/11/23.
 */
public class ClearHandler extends SourceHandler {
    @AutoMatch
    private ClearHelper clearHelper;
    private int batchDelSize=Integer.parseInt(PropertiesUtils.getCommonYaml("task.batch.delsize"));//批量删除size
    private int timeout=Integer.parseInt(PropertiesUtils.getCommonYaml("task.batch.timeout"));//队列中获取记录时最多间隔时间。

    private static final String SEPARATOR="_";
    @Override
    public void process(Map<String, Object> map, List<TaskResult> list) throws Exception {
        //行政区码_终端地址_测量点序号_采集日期（yyyymmddhhmmss）_数据项标示
        List<Object[]> cpList=new ArrayList<>();
        Queues.drain(PropertiesUtils.clearQueue,cpList,batchDelSize,timeout, TimeUnit.SECONDS);
        if(cpList==null||cpList.size()==0){
            return;
        }
        List<String> cps=new ArrayList<>();
        translate(cpList,cps);

       int count= clearHelper.process(cps);
        System.out.println(count);
    }

    /**
     * 采集点信息转换 入参格式：数组内容为[行政区码,终端地址,测量点序号,采集日期（yyyymmddhhmmss）,AFN,FN]
     * 出参格式：行政区码_终端地址_测量点序号_采集日期（yyyymmddhhmmss）_数据项标示
     *
     * @param cps
     * @return
     */
    private static void translate(List<Object[]> cps, List<String> newCps) {
        for (int i = 0; i < cps.size(); i++) {
            Object[] cp = cps.get(i);
            String _tempCp = StringUtil.arrToStr(SEPARATOR, cp[0], cp[1], cp[2], cp[3], cp[4]);
            newCps.add(_tempCp);
        }
        cps.clear();
    }



}
