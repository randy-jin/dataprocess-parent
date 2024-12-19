package com.tl.promethues;

import io.prometheus.client.Gauge;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;

public class PromethuesAllDayUtil {

//	public static final Gauge sendTotalAllDay = Gauge.build().name("send_allDay_total").help("xx").
//			labelNames("total").register();

//	public static final Gauge sendTaskIDAllDay = Gauge.build().name("send_allDay_taskId").help("xx").
//			labelNames("taskId").register();
	
	public static final Gauge fromRedisCountAllDay = Gauge.build().name("from_redis_count").help("xx").
			labelNames("type").register();
	
	public static final Gauge fromRedisCountFnAllDay = Gauge.build().name("from_Redis_fncount").help("xx").
			labelNames("type","fn").register();
	
	public static final Gauge toDataHubCountAllDay = Gauge.build().name("to_dataHub_count").help("xx").
			labelNames("type","itemIdOrAFN").register();

	private String time;

	public void run() throws ParseException {

		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

		String dq = simpleDateFormat.format(new Date()).substring(0, 10);

		Date date = simpleDateFormat.parse(dq + " " + time);

		if (date.before(new Date())) {

			date = addDay(date, 1);

		}

		Timer timer = new Timer();

		timer.schedule(new TimerTask() {

			@Override
			public void run() {

//				sendTotalAllDay.clear();

				fromRedisCountAllDay.clear();
				
				fromRedisCountFnAllDay.clear();
				
				toDataHubCountAllDay.clear();
			}
		}, date, 1000*60*60*24);
	}


	public static Date addDay(Date date, int num) {

		Calendar startDT = Calendar.getInstance();

		startDT.setTime(date);

		startDT.add(Calendar.DAY_OF_MONTH, num);

		return startDT.getTime();
	}

	public String getTime() {
		return time;
	}

	public void setTime(String time) {
		this.time = time;
	}
}

