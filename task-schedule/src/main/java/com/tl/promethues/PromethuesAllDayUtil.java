package com.tl.promethues;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;

import io.prometheus.client.Gauge;

public class PromethuesAllDayUtil {

//	public static final Gauge sendTotalAllDay = Gauge.build().name("send_allDay_total").help("xx").
//			labelNames("total").register();

	public static final Gauge sendTaskIDAllDay = Gauge.build().name("send_allDay_taskId").help("xx").
			labelNames("taskId").register();

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

				sendTaskIDAllDay.clear();
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

