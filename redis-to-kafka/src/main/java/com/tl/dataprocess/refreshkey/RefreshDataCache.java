package com.tl.dataprocess.refreshkey;

import com.ls.pf.base.utils.tools.LinkedBlockingQueue;
import com.tl.easb.utils.CacheUtil;
import org.apache.log4j.Logger;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class RefreshDataCache {
	private final static Logger logger = Logger.getLogger(RefreshDataCache.class);
	private static ThreadLocal<SimpleDateFormat> currentFormat = new ThreadLocal<SimpleDateFormat>();

	LinkedBlockingQueue<Object[]> queue = new LinkedBlockingQueue<Object[]>();
	private int interval = 10;
	private boolean refresh = true;// 默认是刷新缓存的，这里就刷新可能会带来性能问题，这里做一个配置。

	public void refresh(Object[] objs) {
		if (refresh) {
			String sdate = "00000000000000";
			if (objs[2] != null && objs[2] instanceof Date) {
				sdate = getFormat().format((Date) objs[2]);
			} else {
				if (!objs[2].equals(sdate)) {
					logger.error("Date is null or Date format error, but the value is not " + sdate
							+ ", the real value is: " + objs[2]);
				}
			}
			objs[2] = sdate;
			queue.add(objs);
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
