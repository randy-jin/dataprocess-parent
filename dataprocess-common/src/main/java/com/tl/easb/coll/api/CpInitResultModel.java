package com.tl.easb.coll.api;

import java.util.List;

public class CpInitResultModel {
	private long handledSum=0;
	private List<String> existedCps = null;
	public long getHandledSum() {
		return handledSum;
	}
	public void setHandledSum(long handledSum) {
		this.handledSum = handledSum;
	}
	public List<String> getExistedCps() {
		return existedCps;
	}
	public void setExistedCps(List<String> existedCps) {
		this.existedCps = existedCps;
	}
	
}
