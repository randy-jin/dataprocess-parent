package com.tl.dataprocess;

import com.tl.common.seda.handler.TaskHandler;
import com.tl.common.seda.handler.TaskResult;
import com.tl.common.seda.task.AutoMatch;
import com.tl.common.seda.task.Task;
import com.tl.common.seda.task.TaskFailHandler;
import com.tl.datahub.DataHubShardCache;
import com.tl.dataprocess.utils.DataObject;
import com.tl.utils.helper.DatahubHelper;

import java.util.List;

/**
 * Created by huangchunhuai on 2021/11/23.
 */
public class ToDatahubHandler extends TaskHandler{
    @AutoMatch
    private DatahubHelper datahubHelper;


    @Override
    public void process(Task task, List<TaskResult> list) throws Exception {
        List<DataObject> dataObjectList= (List<DataObject>) task.getObj();
        datahubHelper.insert(dataObjectList);
    }

    @Override
    public TaskFailHandler getTaskFailHanlder() {
        return null;
    }
}
