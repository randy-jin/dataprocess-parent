package com.tl.easb.task.job.common.exec;

import java.lang.reflect.Method;

import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import com.tl.easb.task.job.common.CommonBaseJob;
import com.tl.easb.utils.SpringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * quartz调用java程序基类
 * @author JinZhiQiang
 * @date 2014年4月25日
 */
public class ProgramExecJob extends CommonBaseJob {
	private static Logger log = LoggerFactory.getLogger(ProgramExecJob.class);

	@Override
	public void onExecute() {
		if(StringUtils.isBlank(getTriggerName())){
			return;
		}
		ProgramExecJob obj = (ProgramExecJob)SpringUtils.getBean(getTriggerName().trim());
		if(StringUtils.isBlank(obj.getClassName()) || StringUtils.isBlank(obj.getMethodName())){
			return;
		}
		Object[] params = null;
		if(null != getParams()){
			params = obj.getParams().split(",");
		}
		try {
			Class<?> cls = Class.forName(obj.getClassName());
			Method[] methods = cls.getDeclaredMethods();
			for(int i=0;i<methods.length;i++){
				Method method = methods[i];
				if(method.getName().equals(obj.getMethodName())){
					Class<?>[] clses = method.getParameterTypes();
					if(clses.length == 0){
						method.invoke(cls,null);
					} else {
						method.invoke(cls,params);
					}

				}
			}
		} catch (Exception e) {
			log.error("quartz调用Java方法异常：", e);
		}
	}

}
