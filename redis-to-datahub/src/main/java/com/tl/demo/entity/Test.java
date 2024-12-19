package com.tl.demo.entity;

import com.tl.easb.utils.DateUtil;

import java.util.Date;

public class Test {

	public static void main(String[] args) {
		Date date = new Date();
		String dateTime = DateUtil.format(date, "yyyy-MM")+"-01";
		System.out.println(dateTime);
	}	

}
