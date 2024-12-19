package com.tl.easb.task.handle.subtask;

import java.util.HashMap;

public class StringHashCodeTest {

	static HashMap<Integer,String> map = new HashMap<Integer,String>();

	private static int dup = 0;

	public static void main(String[] args) {
		int count = 0;
		for(int areaCode = 100 ;areaCode<2000;areaCode++){
			for(int terminalAddr = 20000;terminalAddr<30000;terminalAddr++){
				count ++;
				String doc = "M$"+areaCode+"#"+terminalAddr;
				int hash = doc.hashCode();
				if(map.containsKey(hash)){
					String s = (String) map.get(hash);
					dup ++;
					System.out.println(s + ":" + doc); 
				} else {
					map.put(hash, doc);
				}
			}
		}
		System.out.println(count + ":" + dup); 
	}
}
