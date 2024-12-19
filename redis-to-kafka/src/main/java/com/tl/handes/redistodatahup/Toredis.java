package com.tl.handes.redistodatahup;

import redis.clients.jedis.Jedis;

import java.util.Random;

public class Toredis {
		private static Jedis jedis = null;
		public  void step(){
			jedis = new Jedis("127.0.0.1",6379);
		}
		public void to(){
			//Long times = System.currentTimeMillis();
			Random r = new Random();
			int n = r.nextInt(9999999);
			n = Math.abs(r.nextInt() % 10000000);
			String val =  String.valueOf(n);
			System.out.println(jedis.lpush("ooo",val));//返回它在list中的位置序号
		}
		  
		public static void main(String[] args) throws Exception{
			Toredis obj = new Toredis();
			int flag = 0;
			obj.step();
			while(true){
				obj.to();
				flag++;
				if(flag >20) {
					flag = 0;
					Thread.sleep(10000);//每10秒写入20个数据到redis
				}
			}
			 
		}
	
	
	
	
}
