package com.tl.eas.common.cache.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;

public class SerializeTest {
	public static void main(String args[]) {
		
//		CacheRedisSerializer javaSerial = new CacheRedisSerializer();
		
		try {
//			byte[] bt = javaSerial.serialize("jinzhiqiang");
//			for(byte b:BytesCoverUtil.coverToData("jinzhiqiang")){
//				System.out.println(b);
//			}
//			System.out.println(bt);
			byte[] data = { 2, 9, 11, 106, 105, 110, 122, 104, 105, 113, 105, 97, 110, 103 };
			String str = new String(data);
			System.out.println(str);
			
			ByteArrayInputStream in = new ByteArrayInputStream(data);
//			in.read();// skip sign
			ObjectInputStream stream = new ObjectInputStream(in);
			Object result = stream.readObject();
			stream.close();
			in.close();
			System.out.println(result);
			
//			System.out.println(BytesCoverUtil.coverToValue(data));
//			Object obj = javaSerial.deserialize(BytesCoverUtil.coverToData("jinzhiqiang"));
//			System.out.println(obj);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
//		SerializeTest jt = new SerializeTest();
//
//		String s = "helloworld!";
//		byte[] bt = null;
//		bt = s.getBytes();
//		ArrayList<String> list = new ArrayList<String>();
//		list = jt.getArrayList(bt);// 这一行会出错。

	}
	
	
	private byte[] ObjectToByte(Object obj) {  
        byte[] bytes = null;  
        try {  
            // object to bytearray  
            ByteArrayOutputStream bo = new ByteArrayOutputStream();  
            ObjectOutputStream oo = new ObjectOutputStream(bo);  
            oo.writeObject(obj);  
      
            bytes = bo.toByteArray();  
      
            bo.close();  
            oo.close();  
        } catch (Exception e) {  
            System.out.println("translation" + e.getMessage());  
            e.printStackTrace();  
        }  
        return bytes;  
    } 
    
    /**
     * byte转对象
     * @param bytes
     * @return
     */
    private Object ByteToObject(byte[] bytes) {
        Object obj = null;
        try {
            // bytearray to object
            ByteArrayInputStream bi = new ByteArrayInputStream(bytes);
            ObjectInputStream oi = new ObjectInputStream(bi);

            obj = oi.readObject();
            bi.close();
            oi.close();
        } catch (Exception e) {
            System.out.println("translation" + e.getMessage());
            e.printStackTrace();
        }
        return obj;
    }

	public ArrayList<String> getArrayList(byte[] bt) {
		ArrayList<String> list = new ArrayList<String>();

		// 注意这里，ObjectInputStream 对以前使用 ObjectOutputStream
		// 写入的基本数据和对象进行反序列化。问题就在这里，你是直接将byte[]数组传递过来，而这个byte数组不是使用ObjectOutputStream类写入的。所以问题解决的办法就是：用输出流得到byte[]数组。
		try {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(baos);
			oos.write(bt);
			byte[] str = baos.toByteArray();

			ObjectInputStream objIps = new ObjectInputStream(new ByteArrayInputStream(str));
			list = (ArrayList<String>) objIps.readObject();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return list;
	}
}
