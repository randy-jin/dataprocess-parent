package com.tl.easb.utils;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;


public class DateUtil {

    private static Logger log = LoggerFactory.getLogger(DateUtil.class);
    /**
     * 日期格式化key
     */
    public static final int FMT_DATE_YYYY_MM_DD = 1;

    public static final int FMT_DATE_YYYYMMDD = 2;

    public static final int FMT_DATE_YYMMDD = 3;

    public static final int FMT_DATE_YYYY = 4;

    public static final int FMT_DATE_YYMM = 5;

    public static final int FMT_DATE_YYYYMM = 6;

    public static final int FMT_DATE_YYYYMMDDHHmmss = 7;

    public static final int FMT_DATE_YYMMDDHHmm = 8;

    public static final int FMT_DATE_HHmm = 9;

    public static final String defaultDatePattern_YMD = "yyyyMMdd";
    public static final String defaultDatePattern_YMDHMS = "yyyyMMdd HH:mm:ss";
    public static final String defaultDatePattern_Y_M_D_HMS = "yyyy-MM-dd HH:mm:ss";


    public static final String[] MONTH_NUM = {"01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"};
    public static final String[] MONTH_EN = {"January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"};
    public static final String[] MONTH_ZH = {"一月", "二月", "三月", "四月", "五月", "六月", "七月", "八月", "九月", "十月", "十一月", "十二月"};

    private static String timePattern = "HH:mm";


    /**
     * 获得默认的 date pattern
     */
    public static String getDatePattern() {
        return defaultDatePattern_YMD;
    }

    public static String getDateTimePattern() {
        return DateUtil.getDatePattern() + " HH:mm:ss";
    }

    /**
     * 返回预设Format的当前日期字符串
     */
    public static String getToday() {
        Date today = _getCurDate();
        return format(today);
    }

    /**
     * 使用预设Format格式化Date成字符串
     */
    public static String format(Date date) {
        return date == null ? "" : format(date, getDatePattern());
    }

    /**
     * 使用参数Format格式化Date成字符串
     */
    public static String format(Date date, String pattern) {
        return date == null ? "" : new SimpleDateFormat(pattern).format(date);
    }

    /**
     * 使用预设格式将字符串转为Date
     */
    public static Date parse(String strDate) throws ParseException {
        return StringUtils.isBlank(strDate) ? null : parse(strDate, getDatePattern());
    }

    /**
     * 使用参数Format将字符串转为Date
     */
    public static Date parse(String strDate, String pattern) throws ParseException {
        return StringUtils.isBlank(strDate) ? null : new SimpleDateFormat(pattern).parse(strDate);
    }

    /**
     * 使用参数Format,以国家，将字符串转为Date
     *
     * @param strDate
     * @param pattern
     * @param locale
     * @return
     * @throws ParseException
     */
    public static Date parse(String strDate, String pattern, Locale locale) throws ParseException {
        return StringUtils.isBlank(strDate) ? null : new SimpleDateFormat(pattern, locale).parse(strDate);
    }

    /**
     * 在日期上增加数个整月
     */
    public static Date addMonths(Date date, int n) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        cal.add(Calendar.MONTH, n);
        return cal.getTime();
    }

    /**
     * 在日期上增加数天
     */
    public static Date addDaysOfMonth(Date date, int n) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        cal.add(Calendar.DAY_OF_MONTH, n);
        return cal.getTime();
    }

    /**
     * 在日期上增加数天年
     */
    public static Date addYears(Date date, int n) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        cal.add(Calendar.YEAR, n);
        return cal.getTime();
    }

    /**
     * This method attempts to convert an Oracle-formatted date in the form
     * dd-MMM-yyyy to mm/dd/yyyy.
     *
     * @param aDate date from database as a string
     * @return formatted string for the ui
     */
    public static final String getDate(Date aDate) {
        SimpleDateFormat df = null;
        String returnValue = "";
        if (aDate != null) {
            df = new SimpleDateFormat(getDatePattern());
            returnValue = df.format(aDate);
        }

        return (returnValue);
    }

    /**
     * This method generates a string representation of a date/time in the
     * format you specify on input
     *
     * @param aMask   the date pattern the string is in
     * @param strDate a string representation of a date
     * @return a converted Date object
     * @throws ParseException
     * @see SimpleDateFormat
     */
    public static final Date convertStringToDate(String aMask, String strDate) throws ParseException {
        SimpleDateFormat df = null;
        Date date = null;
        df = new SimpleDateFormat(aMask);
        try {
            date = df.parse(strDate);
        } catch (ParseException pe) {
            // log.error("ParseException: " + pe);
            throw new ParseException(pe.getMessage(), pe.getErrorOffset());
        }

        return (date);
    }

    /**
     * 根据传入年月，返回当前年月所在的季度区间
     *
     * @param _yrmonth
     * @return
     */
    public static final String[] formatQuarterDate(String _yrmonth) {
        String[] retYrmonth = new String[2];
        String _month = _yrmonth.substring(4, 6);
        String _year = _yrmonth.substring(0, 4);
        String _retMonth1 = _month;
        String _retMonth2 = _month;
        int _monthInt = Integer.valueOf(_month);
        switch (_monthInt) {
            case 0:
            case 1:
            case 2:
            case 3:
                _retMonth1 = "01";
                _retMonth2 = "03";
                break;
            case 4:
            case 5:
            case 6:
                _retMonth1 = "04";
                _retMonth2 = "06";
                break;
            case 7:
            case 8:
            case 9:
                _retMonth1 = "07";
                _retMonth2 = "09";
                break;
            case 10:
            case 11:
            case 12:
                _retMonth1 = "10";
                _retMonth2 = "12";
                break;
            default:
                _retMonth1 = "01";
                _retMonth2 = "12";
                break;
        }
        retYrmonth[0] = _year + _retMonth1;
        retYrmonth[1] = _year + _retMonth2;
        return retYrmonth;
    }

    /**
     * This method returns the current date time in the format: MM/dd/yyyy HH:MM
     * a
     *
     * @param theTime the current time
     * @return the current date/time
     */
    public static String getTimeNow(Date theTime) {
        return getDateTime(timePattern, theTime);
    }

    /**
     * This method generates a string representation of a date's date/time in
     * the format you specify on input
     *
     * @param aMask the date pattern the string is in
     * @param aDate a date object
     * @return a formatted string representation of the date
     * @see SimpleDateFormat
     */
    public static final String getDateTime(String aMask, Date aDate) {
        SimpleDateFormat df = null;
        String returnValue = "";
        if (aDate == null) {
            ;
        } else {
            df = new SimpleDateFormat(aMask);
            returnValue = df.format(aDate);
        }

        return (returnValue);
    }

    /**
     * This method generates a string representation of a date based on the
     * System Property 'dateFormat' in the format you specify on input
     *
     * @param aDate A date to convert
     * @return a string representation of the date
     */
    public static final String convertDateToString(Date aDate) {
        return getDateTime(getDatePattern(), aDate);
    }

    /**
     * This method converts a String to a date using the datePattern
     *
     * @param strDate the date to convert (in format MM/dd/yyyy)
     * @return a date object
     * @throws ParseException
     */
    public static Date convertStringToDate(String strDate) throws ParseException {
        Date aDate = null;

        try {
            aDate = convertStringToDate(getDatePattern(), strDate);
        } catch (ParseException pe) {
            log.error("", pe);
            throw new ParseException(pe.getMessage(), pe.getErrorOffset());

        }

        return aDate;
    }

    /**
     * 日期格式化方法
     *
     * @param date 要格式化的日期
     * @param nFmt 格式化样式
     * @return
     */
    public static String formatDate(Date date, int nFmt) {
        SimpleDateFormat fmtDate = null;
        switch (nFmt) {
            default:
            case DateUtil.FMT_DATE_YYYY_MM_DD:
                fmtDate = new SimpleDateFormat("yyyy-MM-dd");
                break;
            case DateUtil.FMT_DATE_YYYYMMDD:
                fmtDate = new SimpleDateFormat("yyyyMMdd");
                break;
            case DateUtil.FMT_DATE_YYMMDD:
                fmtDate = new SimpleDateFormat("yyMMdd");
                break;
            case DateUtil.FMT_DATE_YYYY:
                fmtDate = new SimpleDateFormat("yyyy");
                break;
            case DateUtil.FMT_DATE_YYMM:
                fmtDate = new SimpleDateFormat("yyMM");
                break;
            case DateUtil.FMT_DATE_YYYYMM:
                fmtDate = new SimpleDateFormat("yyyyMM");
                break;
            case DateUtil.FMT_DATE_YYYYMMDDHHmmss:
                fmtDate = new SimpleDateFormat("yyyyMMddHHmmss");
                break;
            case DateUtil.FMT_DATE_YYMMDDHHmm:
                fmtDate = new SimpleDateFormat("yyMMddHHmm");
                break;
            case DateUtil.FMT_DATE_HHmm:
                fmtDate = new SimpleDateFormat("HHmmss");
        }
        return fmtDate.format(date);
    }

    /**
     * @return 返回当前时间 格式:yyyyMMddHHmmss
     */
    public static String getCurrentDateTime() {
        return DateUtil.formatDate(_getCurDate(), DateUtil.FMT_DATE_YYYYMMDDHHmmss);
    }

    /**
     * @return 返回当前时间 格式:HHmmss
     */
    public static String getCurrentTime() {
        return DateUtil.formatDate(_getCurDate(), DateUtil.FMT_DATE_HHmm);
    }

    /**
     * @return 返回当前时间 格式:yyyyMMdd
     */
    public static String getCurrentYMD() {
        return DateUtil.formatDate(_getCurDate(), DateUtil.FMT_DATE_YYYYMMDD);
    }

    /**
     * @return 返回当前时间 格式:参数pattern指定的格式
     */
    public static String getCurrentDate(String pattern) {
        return DateUtil.format(_getCurDate(), pattern);
    }

    /**
     * @return 返回昨天日期 格式:yyyyMMdd
     */
    public static String getPreCurrentDate() {
        Date curDate = _getCurDate();
        Calendar cal = Calendar.getInstance();
        cal.setTime(curDate);
        cal.add(Calendar.DAY_OF_MONTH, -1);
        //		curDate.setDate(curDate.getDate() - 1);
        //		return DateUtil.formatDate(curDate, DateUtil.FMT_DATE_YYYYMMDD);
        return DateUtil.formatDate(cal.getTime(), DateUtil.FMT_DATE_YYYYMMDD);
    }

    /**
     * @return 返回昨天日期 格式:参数pattern指定的格式
     */
    public static String getPreCurrentDate(String pattern) {
        Date curDate = _getCurDate();
        Calendar cal = Calendar.getInstance();
        cal.setTime(curDate);
        cal.add(Calendar.DAY_OF_MONTH, -1);
        //		curDate.setDate(curDate.getDate() - 1);
        //		return DateUtil.format(curDate, pattern);
        return DateUtil.format(cal.getTime(), pattern);
    }

    /**
     * @return 返回当前时间 格式:yyyyMM
     */
    public static String getCurrentYYYYMM() {
        return DateUtil.formatDate(_getCurDate(), DateUtil.FMT_DATE_YYYYMM);
    }

    /**
     * @return 返回当前时间 格式:yyyyMM
     */
    public static String getPreYYYYMM() {
        Date curDate = _getCurDate();
        Calendar cal = Calendar.getInstance();
        cal.setTime(curDate);
        cal.add(Calendar.DAY_OF_MONTH, -1);
        //		curDate.setMonth(curDate.getMonth() - 1);
        return DateUtil.formatDate(cal.getTime(), DateUtil.FMT_DATE_YYYYMM);
    }

    /**
     * 判断当前年月，如果在当前年月是否关帐，如果已关帐则当前年月份，否则返回上一个年月
     *
     * @return
     */
    public static String getCheckedYrMonth() {
        Date checkDate = _getCurDate();
        String day = DateUtil.format(checkDate, "dd");
        Calendar cal = Calendar.getInstance();
        if ("01".equals(day)) {
            cal.setTime(checkDate);
            cal.add(Calendar.MONTH, -1);
            //			checkDate.setMonth(checkDate.getMonth() - 1);
        }
        return DateUtil.format(cal.getTime(), "yyyyMM");
    }

    /**
     * @return 返回当前时间 格式:yyyyMM
     */
    public static String getCurrentYYYY() {
        return DateUtil.formatDate(_getCurDate(), DateUtil.FMT_DATE_YYYY);
    }

    /**
     * 从inFormat格式转换为outFormat格式
     *
     * @param dStr
     * @param inFormat
     * @param outFormat
     * @return
     */
    public static String convert(String dStr, String inFormat, String outFormat) {

        SimpleDateFormat sdf = new SimpleDateFormat(inFormat);
        Date d = null;
        try {
            d = sdf.parse(dStr);
        } catch (ParseException pe) {
            log.error(pe + "");
        }
        return dateToString(d, outFormat);
    }

    /**
     * 按给出的格式将输入的日期转换为字符串
     *
     * @param currdate  输入的时间
     * @param strFormat 　约字的格式
     * @return　按输入时间及约定格式返回的字符串
     */
    public static final String dateToString(Date currdate, String strFormat) {
        String returnDate = "";
        try {
            SimpleDateFormat sdf = new SimpleDateFormat(strFormat);
            if (currdate == null) {
                return returnDate;
            } else {
                returnDate = sdf.format(currdate);
            }
        } catch (NullPointerException e) {
        }
        return returnDate;
    }

    /**
     * 按日期临界值参数返回规则要求的日期（本月/上月） 规则：日期临界值及之后，表示本月；之前表示上月
     *
     * @param dayParam 输入的日期临界值，要求日期为1至31，或者01至31
     * @param pattern  约定的格式
     * @return 按日期临界值参数返回规则要求的日期（本月/上月）
     * @throws Exception
     */

    public static String getFormatDate(String dayParam, String pattern) throws Exception {
        try {
            int dayParamInt = Integer.parseInt(dayParam);
            if (dayParamInt < 1 || dayParamInt > 31) {
                return null;
            } else {
                Date today = _getCurDate();
                String day = format(today, "dd");
                if (Integer.parseInt(day) >= dayParamInt) {
                    return format(today, pattern);
                } else {
                    Date lastMonthDate = DateUtil.addMonths(today, -1);
                    return format(lastMonthDate, pattern);
                }
            }
        } catch (Exception e) {
            throw e;
        }
    }

    /**
     * 按日期临界值参数返回规则要求的日期（本月/上月） 规则：日期临界值及之后，表示上月；之前表示上上月月
     *
     * @param dayParam 输入的日期临界值，要求日期为1至31
     * @param pattern  约定的格式
     * @return 按日期临界值参数返回规则要求的日期（本月/上月）
     * @throws Exception
     */

    public static String getFormatDateLast(String dayParam, String pattern) throws Exception {
        try {
            int dayParamInt = Integer.parseInt(dayParam);
            if (dayParamInt < 1 || dayParamInt > 31) {
                return null;
            } else {
                Date today = _getCurDate();
                String day = new SimpleDateFormat(pattern).format(today);
                if (Integer.parseInt(day) >= dayParamInt) {
                    Date lastMonth = DateUtil.addMonths(today, -1);
                    return format(lastMonth, pattern);
                } else {
                    Date lastMonthDate = DateUtil.addMonths(today, -2);
                    return format(lastMonthDate, pattern);
                }
            }
        } catch (Exception e) {
            throw e;
        }
    }

    /**
     * 根据传入yrMonth字符串获取去年同期的yrMonth，格式为"yyyyMM"
     *
     * @param yrMonParam
     * @return LastYrMon格式"yyyyMM"
     */
    public static String getLastYrMonth(String yrMonParam) {
        String LastYrMon = "";
        String lastYr = yrMonParam.substring(0, 4);
        Integer yr = Integer.parseInt(lastYr) - 1;
        LastYrMon = yr.toString();
        LastYrMon = LastYrMon + yrMonParam.substring(4, 6);
        return LastYrMon;
    }

    /**
     * modify by zw 获取传入日期的 年/月/日/时/分/秒等
     *
     * @param date
     * @param TimeType 格式必须Calendar的属性为：
     *                 如：DateUtil.get(_getCurDate(),Calendar.DAY_OF_MONTH);
     * @return
     */
    public static int get(Date date, int TimeType) {
        Calendar c = Calendar.getInstance();
        c.setTime(date);
        return c.get(TimeType);
    }

    /**
     * 获取日期 获取当前星期举例：DateUtil.getWeek(_getCurDate());
     *
     * @param date
     * @return
     */
    public static String getWeek(Date date) {
        Calendar c = Calendar.getInstance();
        c.setTime(date);
        int week = c.get(Calendar.DAY_OF_WEEK);
        String weekStr = "";
        switch (week) {
            case Calendar.SUNDAY:
                weekStr = "星期日";
                break;
            case Calendar.MONDAY:
                weekStr = "星期一";
                break;
            case Calendar.TUESDAY:
                weekStr = "星期二";
                break;
            case Calendar.WEDNESDAY:
                weekStr = "星期三";
                break;
            case Calendar.THURSDAY:
                weekStr = "星期四";
                break;
            case Calendar.FRIDAY:
                weekStr = "星期五";
                break;
            case Calendar.SATURDAY:
                weekStr = "星期六";
                break;
            default:
                weekStr = "";
                break;
        }
        return weekStr;
    }

    /**
     * 将传入的月份数据返回为对应的语言
     *
     * @param month
     * @param type  "en"：英文 "zh":中文 "num":数字
     * @return
     */
    public static String changeMonthLanguage(String month, String type) {
        int _int_month = Integer.valueOf(month) - 1;
        if (_int_month >= 0 && _int_month < 12) {
            if ("en".equals(type)) {
                return MONTH_EN[_int_month];
            } else if ("zh".equals(type)) {
                return MONTH_ZH[_int_month];
            } else if ("num".equals(type)) {
                return MONTH_NUM[_int_month];
            } else {
                return "";
            }
        } else {
            return "";
        }
    }

    /**
     * 将月份转换为中文月份
     *
     * @param month
     * @return
     */
    public static String changeMonthZh(String month) {
        return changeMonthLanguage(month, "zh");
    }

    /**
     * 将月份转换为英文月份
     *
     * @param month
     * @return
     */
    public static String changeMonthEn(String month) {
        return changeMonthLanguage(month, "en");
    }

    /**
     * 获取当前时间方法，所有取时间都从本方法获取，便于修改
     *
     * @return
     */
    private static Date _getCurDate() {
        return new Date();
    }

    public static Date getCurrentDate() {
        return _getCurDate();
    }

    /**
     * 判断是否闰年
     *
     * @param year
     * @return
     */
    public static boolean isLeapYear(String year) {
        int y = Integer.valueOf(year);
        return ((y % 100 == 0 && y % 400 == 0) || y % 100 != 0 && y % 4 == 0);

    }

	/*public static String[] getDaysByYrMonth(String yrMonth) {
		String[] days28 = { "01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23", "24", "25", "26", "27", "28" };
		String[] days29 = { "01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23", "24", "25", "26", "27", "28",
				"29" };
		String[] days30 = { "01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23", "24", "25", "26", "27", "28",
				"29", "30" };
		String[] days31 = { "01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23", "24", "25", "26", "27", "28",
				"29", "30", "31" };
		String[] months31 = { "01", "03", "05", "07", "08", "10", "12" };
		String[] months30 = { "04", "06", "09", "11" };

		return days31;
	}*/

	/*private static Date _getDBCurDate() {
		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try {
			conn = DBUtils.getConnection();
			pstmt = conn.prepareStatement(DateUtil.DB_DATE_QRY_SQL);
			rs = pstmt.executeQuery();
			Date date = rs.getDate(1);
			return date;
		} catch (Exception e) {
			return _getCurDate();
		} finally {
			DBUtils.close(conn, pstmt, rs);
		}
	}*/

    /**
     * <p>
     * 获得非异常的日期，异常返回空
     * </p>
     *
     * @param second
     * @param minute
     * @param hourOfDay
     * @param date
     * @param month
     * @param year
     * @return
     * @author 曾凡
     * @time 2013-8-15 下午04:44:10
     */
    public static String getDateStr(int second, int minute, int hourOfDay, int date, int month, int year) {
        String dateStr = minute + hourOfDay + date + month + year + second + "";
        if (dateStr != null && !"".equals(dateStr) && !dateStr.contains("E")) {
            return String.format("%d-%02d-%02d %02d:%02d:%02d", year, month, date, hourOfDay, minute, second);
        }
        return null;
    }

    public static Date getYYMMDD(String yymmdd) {
        return getDate("0", "0", "0", yymmdd.substring(4, 6), yymmdd.substring(2, 4), yymmdd.substring(0, 2));
    }

    public static Date getDate(String second, String minute, String hourOfDay, String date, String month, String year) {
        second = second == null || "".equals(second) ? "0" : second;
        minute = minute == null || "".equals(minute) ? "0" : minute;
        hourOfDay = hourOfDay == null || "".equals(hourOfDay) ? "0" : hourOfDay;
        date = date == null || "".equals(date) ? "0" : date;
        month = month == null || "".equals(month) ? "0" : month;
        year = year == null || "".equals(year) ? "0" : "20" + year;
        return getDate(Integer.valueOf(second), Integer.valueOf(minute), Integer.valueOf(hourOfDay), Integer.valueOf(date), Integer.valueOf(month), Integer.valueOf(year));
    }

    /**
     * <p>
     * 获得非异常的日期，异常返回空
     * </p>
     *
     * @author 曾凡
     * @time 2013-8-14 上午09:37:12
     */
    public static Date getDate(int second, int minute, int hourOfDay, int date, int month, int year) {
        Calendar cal = Calendar.getInstance();
        /* 如果没有年则取当前年 */
        year = year == 0 ? cal.get(Calendar.YEAR) : year;
        StringBuilder dateStr = new StringBuilder();
        dateStr.append(minute);
        dateStr.append(hourOfDay);
        dateStr.append(date);
        dateStr.append(month);
        dateStr.append(year);
        dateStr.append(second);
        Date date1 = null;
        String time = null;
        String dateFormat = null;
        String timeFormat = null;
        if (dateStr != null && !"".equals(dateStr) && !dateStr.toString().contains("E")) {
            if (date != 0) {
                dateFormat = "yyyy-MM-dd HH:mm:ss";
                timeFormat = "%d-%02d-%02d %02d:%02d:%02d";
                time = String.format(timeFormat, year, month, date, hourOfDay, minute, second);
            } else {
                dateFormat = "yyyy-MM";
                timeFormat = "%d-%02d";
                time = String.format(timeFormat, year, month);
            }
            try {
                date1 = convertStringToDate(dateFormat, time);
            } catch (ParseException e) {
                log.error("", e);
            }
        }
        return date1;
    }

    /**
     * 获取两个时间点之间的时间列表集合
     * <p>
     * </p>
     *
     * @author 靳治强
     * @time 2013-9-10 下午03:01:29
     */
    public static ArrayList<String> getDateList(String beginDateStr, String endDateStr, String splitStr) {

        SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd");

        String sRet = "";

        Date beginDate = null;
        Date endDate = null;
        GregorianCalendar beginGC = null;
        GregorianCalendar endGC = null;
        ArrayList<String> list = new ArrayList<String>();
        try {

            beginDate = f.parse(beginDateStr);
            endDate = f.parse(endDateStr);

            beginGC = new GregorianCalendar();
            beginGC.setTime(beginDate);
            endGC = new GregorianCalendar();
            endGC.setTime(endDate);

            while (beginGC.getTime().compareTo(endGC.getTime()) <= 0) {
                sRet = beginGC.get(Calendar.YEAR) + splitStr
                        + ((String.valueOf(beginGC.get(Calendar.MONTH) + 1)).length() < 2 ? "0" + String.valueOf(beginGC.get(Calendar.MONTH) + 1) : String.valueOf(beginGC.get(Calendar.MONTH) + 1))
                        + splitStr + ((String.valueOf(beginGC.get(Calendar.DATE))).length() < 2 ? "0" + String.valueOf(beginGC.get(Calendar.DATE)) : String.valueOf(beginGC.get(Calendar.DATE)));
                list.add(sRet);

                beginGC.add(Calendar.DATE, 1);
            }
            return list;
        } catch (Exception e) {
            log.error("", e);
            return null;
        }
    }

    /**
     * 将字符串转换成java.sql.Date
     *
     * @param value
     * @return
     */
    public static java.sql.Date toSqlDate(Object value) {
        java.sql.Date date = null;
        Date date1 = null;
        if (value == null) {
            return null;
        } else {
            try {
                String strFormat = "yyyy-MM-dd";
                SimpleDateFormat df = new SimpleDateFormat(strFormat);
                // java.text.DateFormat df =
                // java.text.DateFormat.getDateInstance();
                date1 = df.parse(value.toString());
                date = new java.sql.Date(date1.getTime());
            } catch (Exception e) {
                return null;
            }
            return date;
        }
    }

    /**
     * 判断采集日期是否大于昨天
     * 返回值：ret<0，表示采集历史数据
     * ret>0,正常采集
     *
     * @param collectDate
     * @return
     */
    public static boolean beforeYesterday(String collectDate) {
        if (collectDate.indexOf("/") < 0) {
            collectDate = collectDate.substring(0, 4) + "/" + collectDate.substring(4, 6) + "/" + collectDate.substring(6, 8);
        }
        boolean flag = true;
        DateFormat df = new SimpleDateFormat("yyyy/MM/dd");
        Date dataDate = new Date();
        Date yesterday = new Date();
        Calendar calendar = new GregorianCalendar();
        calendar.add(Calendar.DATE, -1);
        String yesterdate = df.format(calendar.getTime());
        try {
            dataDate = df.parse(collectDate);
            yesterday = df.parse(yesterdate);
            // dataDate在yesterday之前
            if (dataDate.compareTo(yesterday) < 0) {
                flag = true;
            } else {
                flag = false;
            }
        } catch (ParseException e) {
            log.error(e + "");
        }
        return flag;
    }

}
