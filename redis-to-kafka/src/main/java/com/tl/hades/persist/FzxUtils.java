package com.tl.hades.persist;

import java.text.SimpleDateFormat;
import java.util.Date;

public class FzxUtils {
	/**
	 * 分支箱时间类型计算
	 * @param valueOf
	 * @param date
	 * @return
	 */
	public static Object dataTime(String dataFlag, Date date) {
		SimpleDateFormat sdf=new SimpleDateFormat("HH:mm:ss");
		String dates=sdf.format(date);
		String[] strings=dates.split(":");
		int str1=Integer.parseInt(strings[0]);
		int str2=Integer.parseInt(strings[1]);
		int count=str1*60+str2;
		switch(dataFlag){
		case "1":
			count=count/15;
			break;
		case "2":
			count=count/30;
			break;
		case "3":
			count=count/60;
			break;
		default:
			return null;
		}
		return String.valueOf(count);
	}
}
