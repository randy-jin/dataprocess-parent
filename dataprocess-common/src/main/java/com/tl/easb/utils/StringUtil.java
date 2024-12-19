package com.tl.easb.utils;

import org.apache.commons.lang3.StringUtils;

import java.text.DecimalFormat;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class StringUtil extends StringUtils {

	private StringUtil() {
	}
	
	/**
	 * 将参数列表strs转换成以separator分割的字符串
	 * @param separator
	 * @param strs
	 * @return
	 */
	public static String arrToStr(String separator,Object... strs){
		if(null == separator || null == strs){
			return null;
		}
		StringBuilder sb = new StringBuilder();
		for(Object str : strs){
			sb.append(str).append(separator);
		}
		return sb.substring(0, sb.length() - 1);
	}

	/**
	 * <p>
	 * 按照size在头部填充0
	 * </p>
	 * 
	 * @param rawStr
	 *            原始的字符串
	 * @param size
	 *            补充0后的字符串的长度
	 * @return
	 * @author 曾凡
	 * @time 2013-7-17 下午05:56:37
	 */
	public static String fillZeros(String rawStr, int size) {
		StringBuilder sb = new StringBuilder(rawStr);
		for (int i = rawStr.length(); i < size; i++)
			sb.insert(0, "0");
		return sb.toString();

	}

	public static String[] split(String seperators, String list, boolean include) {
		StringTokenizer tokens = new StringTokenizer(list, seperators, include);
		String result[] = new String[tokens.countTokens()];
		int i = 0;
		while (tokens.hasMoreTokens())
			result[i++] = tokens.nextToken();
		return result;
	}

	public static boolean booleanValue(String tfString) {
		String trimmed = tfString.trim().toLowerCase();
		return trimmed.equals("true") || trimmed.equals("t");
	}

	public static boolean isNotEmpty(String string) {
		return string != null && string.length() > 0;
	}

	public static boolean isEmpty(String string) {
		return string == null || string.length() == 0;
	}

	/**
	 * 根据提供的标示符,分割字符串。
	 * 
	 * @param str
	 * @param delim
	 * @return
	 */
	public static final String[] explodeString(String str, String delim) {
		if (str == null || str.equals("")) {
			String[] retstr = new String[1];
			retstr[0] = "";
			return retstr;
		}
		StringTokenizer st = new StringTokenizer(str, delim);
		String[] retstr = new String[st.countTokens()];
		int i = 0;
		while (st.hasMoreTokens()) {
			retstr[i] = st.nextToken();
			i++;
		}
		if (i == 0) {
			retstr[0] = str;
		}
		return retstr;
	}

	public static String[][] getPKS(String pk, String flag1, String flag2,
			int length2) throws Exception {
		String[][] updatePKS = null;
		String[] updatePK = null;
		String[] pks = null;

		// 对主键数据的主键分组
		updatePK = pk.split(flag1);
		// 初始化释放主键的数组
		updatePKS = new String[updatePK.length][length2];

		// 获取每组主键的值,并存放在指定的数组中
		for (int i = 0, len = updatePK.length; i < len; i++) {
			pks = updatePK[i].split(",");
			for (int j = 0; j < length2; j++) {
				updatePKS[i][j] = pks[j];
			}
		}
		return updatePKS;
	}

	/**
	 * 判断字符串是否非空。
	 * 
	 * @param src
	 *            要判断的字符串。
	 * @return null或长度为0则返回false，否则返回true。
	 */
	public static boolean isNotNull(String src) {
		return (src != null && src.trim().length() > 0);
	}

	/**
	 * 判断某个字符数组是否非空。
	 * 
	 * @param src
	 *            要判断的数组。
	 * @return null或长度为0则返回false，否则返回true。
	 */
	public static boolean isNotNull(String[] src) {
		return (src != null && src.length > 0);
	}

	/**
	 * 过滤空值。
	 * 
	 * @param src
	 *            要过滤的对象。
	 * @param defaultValue
	 *            默认返回值。
	 * @return 过滤null之后的返回值，如果为null，则返回默认值，否则去掉两边的空格后返回。
	 */
	public static String filterNull(Object src, String defaultValue) {
		if (src != null) {
			return src.toString().trim();
		}

		return defaultValue;
	}

	/**
	 * 过滤空值。
	 * 
	 * @param src
	 *            要过滤的字符串。
	 * @param defaultValue
	 *            默认返回值。
	 * @return 过滤null之后的返回值，如果为null，则返回默认值，否则去掉两边的空格后返回。
	 */
	public static String filterNull(String src, String defaultValue) {
		if (src != null) {
			return src.trim();
		}

		return defaultValue;
	}

	/**
	 * 从左边取几个字符。
	 * 
	 * @param src
	 * @param length
	 * @param defaultValue
	 * @return
	 */
	public static String left(String src, int length, String defaultValue) {
		if (src != null) {
			if (src.length() >= length) {
				return src.substring(0, length);
			}

			return src;
		}

		return defaultValue;
	}

	/**
	 * 从左边取几个字符。
	 * 
	 * @param src
	 * @param length
	 * @param defaultValue
	 * @return
	 */
	public static String left(Object src, int length, String defaultValue) {
		if (src != null) {
			String temp = src.toString();

			if (temp.length() >= length) {
				return temp.substring(0, length);
			}

			return temp;
		}

		return defaultValue;
	}

	/**
	 * 从右边取几个字符。
	 * 
	 * @param src
	 * @param length
	 * @param defaultValue
	 * @return
	 */
	public static String right(String src, int length, String defaultValue) {
		if (src != null) {
			int srcLen = src.length();

			if (srcLen >= length) {
				return src.substring(srcLen - length, srcLen);
			}

			return src;
		}

		return defaultValue;
	}

	/**
	 * 从右边取几个字符。
	 * 
	 * @param src
	 * @param length
	 * @param defaultValue
	 * @return
	 */
	public static String right(Object src, int length, String defaultValue) {
		if (src != null) {
			String temp = src.toString();

			int tempLen = temp.length();

			if (tempLen >= length) {
				return temp.substring(tempLen - length, tempLen);
			}

			return temp;
		}

		return defaultValue;
	}

	/**
	 * 按照指定的规则拆分字符串。
	 * 
	 * @param src
	 *            要拆分的字符串。
	 * @param flag
	 *            拆分的标记。
	 * @return 返回拆分后的数组，内容为空也包含在拆分的字符串数组中。
	 */
	public static String[] split(String src, String flag) {
		return StringUtils.split(src, flag);
	}

	/**
	 * 获取不带扩展名的文件名。
	 * 
	 * @param filename
	 *            要处理的文件名。
	 * @return 去掉扩展名后的文件名。
	 */
	public static String getFilenameWithNoExt(String filename) {
		return filename.substring(0, filename.lastIndexOf("."));
	}

	/**
	 * 获取文件的扩展名。
	 * 
	 * @param filename
	 *            要处理的文件名。
	 * @return 文件名的扩展名。
	 */
	public static String getFilenameExt(String filename) {
		return filename.substring(filename.lastIndexOf(".") + 1);
	}

	/**
	 * 将某个值转成整型，如果出错则以默认值返回。
	 * 
	 * @param src
	 * @param defaultValue
	 * @return
	 */
	public static int toInt(String src, int defaultValue) {
		try {
			return Integer.parseInt(src);
		} catch (Exception e) {
			return defaultValue;
		}
	}

	/**
	 * 将某个值转成整型，如果出错则以默认值返回。
	 * 
	 * @param src
	 * @param defaultValue
	 * @return
	 */
	public static int toInt(Object src, int defaultValue) {
		try {
			return Integer.parseInt(src.toString());
		} catch (Exception e) {
			return defaultValue;
		}
	}


	// 字符到字节转换
	public static byte[] charToByte(char ch) {
		int temp = (int) ch;
		byte[] b = new byte[2];
		for (int i = b.length - 1; i > -1; i--) {
			b[i] = new Integer(temp & 0xff).byteValue(); // 将最高位保存在最低位
			temp = temp >> 8; // 向右移8位
		}
		return b;
	}

	// 字节到字符转换

	public static char byteToChar(byte[] b) {
		int s = 0;
		if (b[0] > 0)
			s += b[0];
		else
			s += 256 + b[0];
		s *= 256;
		if (b[1] > 0)
			s += b[1];
		else
			s += 256 + b[1];
		char ch = (char) s;
		return ch;
	}

	// 浮点到字节转换
	public static byte[] doubleToByte(double d) {
		byte[] b = new byte[8];
		long l = Double.doubleToLongBits(d);
		for (int i = 0; i < b.length; i++) {
			b[i] = new Long(l).byteValue();
			l = l >> 8;

		}
		return b;
	}

	// 字节到浮点转换
	public static double byteToDouble(byte[] b) {
		long l;

		l = b[0];
		l &= 0xff;
		l |= ((long) b[1] << 8);
		l &= 0xffff;
		l |= ((long) b[2] << 16);
		l &= 0xffffff;
		l |= ((long) b[3] << 24);
		l &= 0xffffffffl;
		l |= ((long) b[4] << 32);
		l &= 0xffffffffffl;

		l |= ((long) b[5] << 40);
		l &= 0xffffffffffffl;
		l |= ((long) b[6] << 48);

		l |= ((long) b[7] << 56);
		return Double.longBitsToDouble(l);
	}


	public static boolean isBlank(String str) {
		if (str == null || str.length() == 0)
			return true;
		else
			return false;
	}

	/**
	 * 校验是否手机号码
	 * 
	 * @param phone
	 * @return
	 */
	public static boolean validateMoblie(String phone) {
		if (isBlank(phone))
			return false;
		int l = phone.length();
		boolean rs = false;
		switch (l) {
		case 11:
			if (matchingText("^(13[0-9]|15[0-9]|18[7|8|9|6|5])\\d{4,8}$", phone)) {
				rs = true;
			}
			break;
		default:
			rs = false;
			break;
		}
		return rs;
	}

	private static boolean matchingText(String expression, String text) {
		Pattern p = Pattern.compile(expression); // 正则表达式
		Matcher m = p.matcher(text); // 操作的字符串
		boolean b = m.matches();
		return b;
	}

	/**
	 * 格式化Double类型的字符
	 * 
	 * @param d
	 * @param fmtPatten
	 *            格式化样式，依据DecimalFormat的样式，如：#.00
	 * @return
	 */
	public static String doubleToString(Double d, String fmtPatten) {
		DecimalFormat df = new DecimalFormat(fmtPatten);
		String fmtValue = df.format(d);

		return fmtValue;
	}
	
	public static int parseInt(Object rlt) {
		if (rlt == null || "".equals(rlt)) {
			return 0;
		}
		return Integer.parseInt(String.valueOf(rlt));
	}
	
	public static String parseString(Object rlt){
		if(rlt == null || "".equals(rlt)){
			return null;
		} else {
			return String.valueOf(rlt);
		}
	}
	
}
