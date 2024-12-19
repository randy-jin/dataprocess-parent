package com.tl.utils.refreshkey;

import com.tl.utils.helper.SentinelRedisHelper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.scripting.support.ResourceScriptSource;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by huangchunhuai on 2021/11/24.
 */
@Component
public class ClearHelper {

    @Autowired
    SentinelRedisHelper sentinelRedisHelper;

    /**
     * 测点信息数据集（测点--二级任务对应关系集)唯一标识.
     */
    private final static String KEY_CP2SUBTASK = "CP2SUBTASK";
    /**
     * 采集任务测点数据集KEY前缀
     */
    private final static String KPREF_SUBTASK2CP = "STCP:";
    /**
     * 一级任务更新时间
     */
    private final static String KPREF_TASKUPDTIME = "TKUPDTIME:";

    /**
     * 更新任务时间，清理已完成的任务
     * @author wjj
     * @date 2021/11/25 11:03
     * @param cps        被成功采集的测点标识列表
     * @return
     */
    public int process(List<String> cps) {
        if (cps == null || cps.isEmpty()) {
            return 0;
        }
        updTime(cps);
        batchProcess(cps);
        return cps.size();
    }

    /**
     * 批量更新时间戳
     * @param subCps
     */
    private  void updTime(List<String> subCps) {
        List<String> keys = new ArrayList<String>();
        keys.add(String.valueOf(subCps.size()));
        keys.add(KEY_CP2SUBTASK);
        keys.add(KPREF_TASKUPDTIME);
        keys.add(System.currentTimeMillis() + "");
        DefaultRedisScript redisScript = new DefaultRedisScript();
        redisScript.setScriptSource(new ResourceScriptSource(new ClassPathResource("lua/TaskUpTime.mylua")));
        sentinelRedisHelper.redisTemplate.execute(redisScript, keys, subCps.toArray());
    }

    /**
     * 将用于更新时间戳的测量点在任务缓存中清除
     * @param subcps
     */
    private  void batchProcess(final List<String> subcps) {
        List<String> keys = new ArrayList<String>();
        keys.add(String.valueOf(subcps.size()));
        keys.add(KEY_CP2SUBTASK);
        keys.add(KPREF_SUBTASK2CP);
        DefaultRedisScript redisScript = new DefaultRedisScript();
        redisScript.setScriptSource(new ResourceScriptSource(new ClassPathResource("lua/ProcedureBatchProcess.mylua")));
        sentinelRedisHelper.redisTemplate.execute(redisScript, keys, subcps.toArray());
    }

}
