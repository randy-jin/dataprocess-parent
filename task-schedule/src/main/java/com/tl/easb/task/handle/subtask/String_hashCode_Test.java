package com.tl.easb.task.handle.subtask;

import java.util.HashMap;

public class String_hashCode_Test {
	static HashMap map = new HashMap(); 

    private static char startChar = 'A'; 

    private static char endChar = 'z'; 

    private static int offset = endChar - startChar + 1; 

    private static int dup = 0; 

    public static void main(String[] args) {
        int len = 3; 
        char[] chars = new char[len]; 
        tryBit(chars, len); 
        System.out.println((int)Math.pow(offset, len) + ":" + dup); 
    } 

    private static void tryBit(char[] chars, int i) {
        for (char j = startChar; j <= endChar; j++) {
            chars[i - 1] = j; 
            if (i > 1) 
                tryBit(chars, i - 1); 
            else 
                test(chars); 
        } 
    } 

    private static void test(char[] chars) {

//        String str = new String(chars).replaceAll("[^a-zA-Z_]", "").toUpperCase();// 195112:0 
//        String str = new String(chars).toLowerCase();//195112:6612 
//        String str = new String(chars).replaceAll("[^a-zA-Z_]","");//195112:122500 
        String str = new String(chars);//195112:138510 
        int hash = str.hashCode();
        if (map.containsKey(hash)) {
            String s = (String) map.get(hash); 
            if (!s.equals(str)) { 
            	dup++; 
            	System.out.println(s + ":" + str); 
            }
        } else {
        	map.put(hash, str); 
            // System.out.println(str); 
        } 
    } 
}
