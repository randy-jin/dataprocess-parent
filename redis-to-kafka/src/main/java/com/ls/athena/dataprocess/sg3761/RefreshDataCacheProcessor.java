package com.ls.athena.dataprocess.sg3761;

import com.ls.athena.core.MessageContext;
import com.ls.athena.core.basic.UnsettledMessageProcessor;
import com.ls.pf.base.utils.tools.LinkedBlockingQueue;
import com.tl.easb.utils.CacheUtil;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class RefreshDataCacheProcessor extends UnsettledMessageProcessor {
	private static ThreadLocal<SimpleDateFormat> currentFormat = new ThreadLocal<SimpleDateFormat>();

	public static LinkedBlockingQueue<Object[]> queue = new LinkedBlockingQueue<Object[]>();
	private int interval = 10;
	private boolean refresh = false;// 默认是刷新缓存的，这里就刷新可能会带来性能问题，这里做一个配置。

	@Override
	protected boolean doCanProcess(MessageContext context) throws Exception {
		Object obj = context.getContent();
		if (obj instanceof Object[]) {
			Object[] objs = (Object[]) obj;
			for (Object o : objs) {            //从数组中取对象 content=objs=list -->list1=o
				Object[] check = (Object[]) o;//将取出的对象转为数组list1=o
				if (check[0] != null && check[0] instanceof Object[]) { //list1[0]=o[0]=check[0]
					for (Object z : (Object[]) o) {//取list1的数据 z
						refresh((Object[]) z);//刷新这个数据
					}
				} else {
					refresh((Object[]) o);//如果首元素不为list，就刷新这条数据 
				}
			}
			return true;
		}
		return false;
	}

	private void refresh(Object[] objs) {
		if (refresh) {
			if (objs != null) {
				String sdate = "00000000000000";
				if (objs[3] != null) {
					sdate = getFormat().format((Date) objs[3]);
				}
				objs[3] = sdate;
				queue.add(objs);
			}
		}
	}

	class RefreshThread implements Runnable {

		public void run() {
			while (true) {
				try {
					List<Object[]> list = new ArrayList<Object[]>(queue.size());
					int size = queue.drainTo(list);
					if (size > 0) {
						CacheUtil.process(list);
					}
					Thread.sleep(interval * 1000);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}

	public void init() {
		Thread t = new Thread(new RefreshThread());
		t.setName("RefreshCacheThread");
		t.start();
	}

	static SimpleDateFormat getFormat() {
		SimpleDateFormat format = currentFormat.get();
		if (null == format) {
			format = new SimpleDateFormat("yyyyMMddHHmmss");
			currentFormat.set(format);
		}
		return format;
	}
	
	@Override
	protected boolean doCanFinish(MessageContext context) throws Exception {
		return true;
	}

	public int getInterval() {
		return interval;
	}

	public void setInterval(int interval) {
		this.interval = interval;
	}

	public boolean isRefresh() {
		return refresh;
	}

	public void setRefresh(boolean refresh) {
		this.refresh = refresh;
	}
}
