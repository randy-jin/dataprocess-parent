package com.tl.test;

import com.tl.easb.utils.DateUtil;
import redis.clients.jedis.Jedis;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;


public class T1 {
    private static final long PERIOD = 3000;

    public static boolean allNull(List list) { //传入了当前pnfn测量点的数据项列表list
        boolean allNull = true;
        for (Object o : list) {
            if (o instanceof List) {
                allNull((List) o);
            } else {
                if (o != null
                        && !(o instanceof Integer)) {
                    allNull = false;
                    return allNull;
                }
            }
        }
        return allNull;
    }

    public static void main(String[] args) throws Exception {

        System.out.println(DateUtil.getPreCurrentDate(DateUtil.defaultDatePattern_YMD));

        System.out.println(new Date(1636931565955L));

//        long getPurchaseTime = 1616171834000l;
//        long getPurchaseTimeEscaped = 1000;
//
//        if (getPurchaseTimeEscaped > 30 ) {
//            if (getPurchaseTimeEscaped < 60) {
//                System.out.println("next");
//            } else {
//                System.out.println("exec");
//            }
//        } else if (getPurchaseTimeEscaped > 100) {
//            System.out.println("fail");
//        }
//        System.out.println(getPurchaseTimeEscaped);
//		String date="2000-00-00 00:00";
//		SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm");
//		System.out.println(sdf.parse(date));

//		Object obj_v=new Date();
//		if(!(obj_v instanceof Date)){
//			System.out.println(11);
//		}
//		List alllList=new ArrayList<>();
//		String nodeInfo="10100100";
//		alllList.add(nodeInfo.substring(0,1));//表档案状态
//		alllList.add(nodeInfo.substring(1,3));//台区状态
//		alllList.add(nodeInfo.substring(3,6));//相序
//		alllList.add(nodeInfo.substring(6,7));//线路状态
//		for(Object o:alllList){
//			System.out.println(o);
//		}
//		System.out.println(new Date(943891200000L));
//		String date="2019-03-14&04&17:37:14";
//		String[] str=date.split("&04&");
//		if(str.length!=2){
////			continue;
//		}
//		StringBuffer sb=new StringBuffer();
//		sb.append(str[0]);
//		sb.append(" ");
//		sb.append(str[1]);
//		SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//		System.out.println(sdf.parse(sb.toString()));;
//		StringBuffer statusStr1=new StringBuffer();
//		System.out.println(statusStr1.toString());
//		StringBuffer statusStr=new StringBuffer();
//		List a=new ArrayList();
//		a.add("0");
//		a.add("1");
//		a.add("2");
//		a.add("3");
//		a.add("4");
//		a.add("5");
//		a.add("6");
//		a.add("0");
//		a.add("8");
//		a.add("9");
//		a.add("a");
//		a.add("b");
//		
//		a.add("s");
//		a.add("s");
//		for (int j = 0; j < a.size()-2; j++) {
//			if(j==1||j==7){
//				String st=a.get(j).toString();
//				if(st.equals("0")||st.equals("1")){
//					statusStr.append("0").append(a.get(j));
//				}else if(st.equals("2")){
//					statusStr.append("10");
//				}
//			}else {
//				statusStr.append(a.get(j));
//			}
//		}
//		System.out.println(statusStr.toString());
//		System.out.println(statusStr.length());
//		Jedis jedis =new Jedis("10.230.27.202",52764);
//		jedis.auth("1qaz2wsx4rfv");
//		System.out.println(jedis.llen("Q_DATA_FREEZE"));
//		Date demandDate =new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").parse("1901-01-01 00:00:00");
//		System.out.println(demandDate);
//		List l=new ArrayList();
//		List l1=new ArrayList();
//		l.add("10.0");
//		l1.add("null");
//		l1.add("null");
//		l.add(l1);
//		
//		System.out.println(allNull(l));
//		int a[]= new int[2];
//		a[0]=49;
//		a[1]=48;
//		l1.add(49);
//		l1.add(57);
//		l1.add(22);
//		l1.add(12);
//		l.add(a);
//		System.out.println(l.get(0));
//		search();
//		del();
//		String st="[302C0200]";
//		System.out.println(st.substring(1,st.length()-1));
//		
//		Map<Object,Object> ob=new HashMap<>();
//		ob.put("20018", new Date());
//		System.out.println((Date)ob.get("20018"));
//		
//		String a="20181228";
//		Date v=DateUtil.parse(a);
//		Calendar c=Calendar.getInstance();
//		c.setTime(v);
//		System.out.println(DateUtil.format(c.getTime(), DateUtil.defaultDatePattern_YMD));
//		Calendar d=Calendar.getInstance();
//		d.set(Calendar.YEAR, c.get(c.YEAR));
//		d.set(Calendar.MONTH, c.get(c.MONTH));
//		int day=d.getActualMaximum(Calendar.DATE);
//		System.out.println(day);
//		d.set(Calendar.DAY_OF_MONTH, day);
//		System.out.println(DateUtil.format(d.getTime(), DateUtil.defaultDatePattern_YMD));
//		String a="456789";
//		System.out.println(a.subSequence(a.length()-1, a.length()));
//		List l1=new ArrayList();
//		l1=null;
//		System.out.println(l1==null);
//		l1.add("");
//		l1.add("22.1");
//		l1.add("2018-12-12");
//		System.out.println(l1.get(l1.size()-1));
//		l1.add(null);
//		l1.add(null);
//		System.out.println(l1.get(0)==null);
//		System.out.println(allNull(l1));
//		System.out.println(null instanceof Integer);
//		Date collDate=new Date();
//		Calendar calendar = Calendar.getInstance();
//		calendar.setTime(collDate);
//		calendar.add(calendar.DAY_OF_MONTH, -1);
//		Date endcollDate=collDate;
//		System.out.println(endcollDate);
//			endcollDate=calendar.getTime();
//		System.out.println(endcollDate);
//		
//		try {
//			Date datebg=new java.util.Date(new SimpleDateFormat("yyyy-MM-dd HH:mm").parse(String.valueOf("2000-00-00 00:00")).getTime());
//			System.out.println(datebg);
//		} catch (ParseException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}


//		Set<String> s=jedis.keys("Q*");
//		for (Iterator iterator = s.iterator(); iterator.hasNext();) {
//			System.out.println(iterator.next());
//			
//		}
//		Object obj=jedis.lpop("MONITOR_MESSAGE_QUEUE");

//		System.out.println(jedis.lrange("Q_DATA_FREEZE",0,-1));
//		Calendar calendar = getCalendar();
//		calendar.setTime(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse("2018-03-01 00:00:00"));
//		calendar.add(calendar.DAY_OF_MONTH, -1);
//		System.out.println(calendar.getTime());
//		Jedis jedis =new Jedis("10.230.26.41",17986);
//		jedis.auth("1qaz2wsx4rfv");
//		jedis.set("a", "a");
//		System.out.println(jedis.ping());
//		String s=jedis.get("TKUPDTIME:500032_20180823000000");
//		long d=Long.parseLong(s);
//		System.out.println(new Date(11101L));
//		String s=null;
//		System.out.println("".equals(s));
//		Map m=new HashMap();
//		m.put("PORT", "2");
//		m.put("PORT1", "2");
//		m.put("PORT1", "3");
//		System.out.println(m.get(null));
//		System.out.println(new Date(1535426299033L));
//			try {
//				while(true){
//					insert();
//					Thread.sleep(10000);
//				}
//			} catch (Exception e) {
//				e.printStackTrace();
//			}
//		search2();
//		List lsit1=new ArrayList<>();
//		List list2=new ArrayList<>();
//		list2.add(1);
//		lsit1=list2;
//		list2=new ArrayList<>();
//		list2.add(2);
//		for(Object o:lsit1){
//			System.out.println(o);
//		}
//		System.out.println(new ArrayList()==null);


//        System.out.println(DateUtil.parse(DateUtil.format(DateUtil.addDaysOfMonth(DateUtil.parse("2021-03-19 00:41:00", "yyyy-MM-dd HH:mm:ss"), 0), DateUtil.defaultDatePattern_YMD)));
//
//        System.out.println(DateUtil.parse("20210319"));

    }

    private static ThreadLocal<Calendar> currentCalendar = new ThreadLocal<Calendar>();
    private static ThreadLocal<SimpleDateFormat> currentFormat = new ThreadLocal<SimpleDateFormat>();

    static Calendar getCalendar() {
        Calendar calendar = currentCalendar.get();
        if (null == calendar) {
            calendar = Calendar.getInstance();
            currentCalendar.set(calendar);
        }
        return calendar;
    }

    private static void search() {
        List<String> redislist = new ArrayList<String>();
        redislist.add("10.230.26.41");
        redislist.add("10.230.26.53");
        redislist.add("10.230.26.54");
        redislist.add("10.230.26.55");
        redislist.add("10.230.26.56");
        int port = 6381;
        String pass = "1qaz2wsx4rfv";
        String redisKey = "MONITOR_MESSAGE_QUEUE";
//		String redisKey="Q_DATA_FREEZE";
        for (String ip : redislist) {
            try {
                Jedis jedis = new Jedis(ip, port);
                jedis.auth(pass);
                Long count = jedis.llen(redisKey);
                System.out.println(jedis.keys("MONITOR*"));
                System.out.println("    " + ip + "          count:" + count);

                jedis.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
//		Jedis jedis1=new Jedis("10.230.27.202", 52764);
//		jedis1.auth(pass);
//		System.out.println(jedis1.keys("*MONITOR*"));
//		jedis1.close();
    }

    private static void search1() {
        String pass = "1qaz2wsx4rfv";
        Jedis jedis1 = new Jedis("10.230.27.202", 52764);
        jedis1.auth(pass);
        System.out.println(jedis1.llen("Q_DATA_CUR"));
        System.out.println(jedis1.keys("*"));
        jedis1.close();
    }

    private static void search2() {
        String pass = "1qaz2wsx4rfv";
        Jedis jedis2 = new Jedis("21.32.65.63", 26381);
        jedis2.auth(pass);
        System.out.println(jedis2.info());
        jedis2.close();
    }

    private static void del() {
        List<String> redislist = new ArrayList<String>();
        redislist.add("10.230.26.41");
        redislist.add("10.230.26.53");
        redislist.add("10.230.26.54");
        redislist.add("10.230.26.55");
        redislist.add("10.230.26.56");
        int port = 6381;
        String pass = "1qaz2wsx4rfv";
        String redisKey = "Q_BASIC_DATA_3XXX";
        for (String ip : redislist) {
            try {
                Jedis jedis = new Jedis(ip, port);
                jedis.auth(pass);
                Long count = jedis.del(redisKey);
                System.out.println("    " + ip + "          count:" + count);
                jedis.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private static void insert() {
        List<String> redislist = new ArrayList<String>();
        redislist.add("10.230.26.41");
        redislist.add("10.230.26.53");
        redislist.add("10.230.26.54");
        redislist.add("10.230.26.55");
        redislist.add("10.230.26.56");
        int port = 6381;
        String pass = "1qaz2wsx4rfv";
        String redisKey = "Q_BASIC_DATA_quxian";
        for (String ip : redislist) {
            try {
                Jedis jedis = new Jedis(ip, port);
                jedis.auth(pass);
                Long count = jedis.llen(redisKey);
                System.out.println(new Date() + "    " + ip + "          count:" + count);
                jedis.close();
            } catch (Exception e) {
            }
        }

    }
}
