package com.tl;

import com.tl.common.seda.handler.SourceHandler;
import com.tl.common.seda.handler.TaskHandler;
import com.tl.common.seda.service.Service;
import com.tl.common.seda.task.Task;
import com.tl.common.seda.task.TaskManager;
import com.tl.dataprocess.*;
import com.tl.queueMapping.QueueMapping;
import com.tl.queueMapping.QueueMappingInit;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public class QueueDataProcessCreator implements Service.ICreator {
    @Override
    public void create(TaskManager taskManager, Map<String, Object> map) {
        List<QueueMapping> queueMappings= QueueMappingInit.getInstance().getAll();
        try {
            for (QueueMapping queueMapping:queueMappings) {
                switch (queueMapping.getHandleType()){
                    case "source":
                        List<Map<String, Object>> param =new ArrayList<>();
                        if(queueMapping.getQueueName()!=null&&queueMapping.getPiplineCount()!=0){
                            Map<String, Object> maps=new HashMap<>(queueMapping.getRunThread());
                            maps.put("queueName",queueMapping.getQueueName());
                            maps.put("piplineCount",queueMapping.getPiplineCount());
                            for (int m =0;m<queueMapping.getRunThread();m++){
                                param.add(maps);
                            }
                        }else{
                            param=null;
                        }

                        Class<?> sourceClass=Class.forName(queueMapping.getClassName()).newInstance().getClass();
                        taskManager.createSourceContainer(queueMapping.getHandleName(),queueMapping.getRunThread(), (Class<? extends SourceHandler>) sourceClass, param);
                        break;
                    case "task":
                        Class<?> taskClass=Class.forName(queueMapping.getClassName()).newInstance().getClass();
                        taskManager.createContainer(queueMapping.getHandleName(), queueMapping.getMaxThread(), queueMapping.getRunThread(), 0, new LinkedBlockingQueue<Task>(), (Class<? extends TaskHandler>) taskClass,queueMapping.getRetryCount());
                        break;
                }
            }
        } catch (Exception e) {
            System.out.println(e);
        }
    }
}
