package com.ls.athena.callmessage.multi.util;

import java.util.List;

public class StringCollectUtil {
	
	
	/****************************************************************
	 * 
	 * @param firstTokens
	 * @param secondTokens
	 * @return
	 * 
	 * 功能说明:
	 *    1.firstTokens和secondTokens 根据符号 | 分割数组 分别为firstAry sencodAry
	 *    2.求firstAry-sencodAry的差集
	 **************************************************************/
	public static String[] subStractSplitByVerticalBar(String firstTokens,String secondTokens){
		if(secondTokens == null || secondTokens.equals("")){
			String[] ntokens = firstTokens.split("\\|");
			return ntokens;
		}
		String[] firstAry = firstTokens.split("\\|");
		String[] sencodAry = secondTokens.split("\\|");
		int firstLen = firstAry.length;
		int secondLen = sencodAry.length;
		String temp = "";
		for(int i=0;i<firstLen;i++){
			int flag = 0;
			for(int j=0;j<secondLen;j++){
				if(firstAry[i].equals(sencodAry[j])){
					flag=1;
					break;
				}
			}
			if(flag==0){
				if(!"".equals(temp)){
					temp = temp+"|"+firstAry[i];
				}
				else{
					temp = firstAry[i];
				}
			}
		}
	
		String[] ntokens = temp.split("\\|");
		return ntokens;
	}

	public static String arrryToStringBySplit(List<String> successList, String string) {
		StringBuffer succBuff= new StringBuffer(); 
		for(int k1 =0;k1<successList.size();k1++) {
			succBuff.append(successList.get(k1)).append("|");
		}
		String succe = succBuff.substring(0, succBuff.length()-1);
		return succe;
	}
	
	public static String fixLenthAppendByZero(String str) {
		if(str.indexOf(".") == -1) {
			if (str.length() < 8) {
				String zs = "00000000".substring(0, 8 - str.length()) + str;
				return zs;
			}
		}else {
			
			String zs = str.substring(0,str.indexOf("."));
			String xs = str.substring(str.indexOf(".")+1);
			
			if (zs.length() < 4) {
				zs = "00000000".substring(0, 4 - zs.length()) + zs;
			}
			
			if (xs.length() < 4) {
				xs =  xs+"00000000".substring(0, 4 - xs.length());
			}
			
			return zs+xs;
			
		}
		return null;

	}
	
	public static String fixLenthAppendByZero2(String str) {
		if(str.indexOf(".") == -1) {
			if (str.length() < 8) {
				String zs = "00000000".substring(0, 8 - str.length()) + str;
				return zs;
			}
		}else {
			String zs = str.substring(0,str.indexOf("."));
			String xs = str.substring(str.indexOf(".")+1);
			
			if (zs.length() < 6) {
				zs = "00000000".substring(0, 6 - zs.length()) + zs;
			}
			
			if (xs.length() < 2) {
				xs =  xs+"00000000".substring(0, 2 - xs.length());
			}
			
			return zs+xs;
		}
		return null;
	}
	
	
	public static String frontAppendByZero(String str,int length) {
		
		if(str.length()<length) {
			return getZeroStr(length-str.length())+str;
		}
		return str;
	}
	
	
	public static String getZeroStr(int len) {
		StringBuffer sb = new StringBuffer();
		for(int i=1;i<=len;i++) {
			sb.append("0");
		}
		return sb.toString();
	}
	
	
	public static void main(String[] args) {
		
		System.out.println(StringCollectUtil.frontAppendByZero("1122", 8));
	}
	
	
	
}
