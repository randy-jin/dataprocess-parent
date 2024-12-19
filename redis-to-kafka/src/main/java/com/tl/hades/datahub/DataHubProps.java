package com.tl.hades.datahub;

import com.aliyun.datahub.DatahubClient;
import com.aliyun.datahub.DatahubConfiguration;
import com.aliyun.datahub.auth.Account;
import com.aliyun.datahub.auth.AliyunAccount;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.net.URL;
import java.net.URLDecoder;
import java.util.Properties;

public class DataHubProps {

	private static final String FILE_NAME = "datahub.conf";
	private static final String ACCESS_ID = "default.access.id";
	private static final String ACCESS_KEY = "default.access.key";
	private static final String ENDPOINT = "default.endpoint";
	private static final String PROJECT = "default.project";

	private static String accessId = null;
	private static String accessKey = null;
	private static String endpoint = null;
	public static String project = null;

	public static DatahubClient client;

	public void init() {
		loadconfig();
		Account account = new AliyunAccount(accessId, accessKey);// 阿里账号认证
		DatahubConfiguration conf = new DatahubConfiguration(account, endpoint);// datahub配置
		client = new DatahubClient(conf);// datahub客户端
//		 DataHubControl.setDatahubClient(client);
	}

	private void loadconfig() {
		URL url = this.getClass().getClassLoader().getResource(FILE_NAME);
		String path = url.toString();
		if (path.startsWith("zip")) { // 判断path是不是以字符串开头
			path = path.substring(4);// 截取掉4个字节，剩下的字节赋值给path
		} else if (path.startsWith("file")) {
			path = path.substring(6);
		} else if (path.startsWith("jar")) {
			path = path.substring(10);
		}
		try {
			path = URLDecoder.decode(path, "UTF-8");// url的转码与解码
			path = path.substring(0, path.length() - 1);
			path = path.substring(0, path.lastIndexOf("/"));// （0，‘/’在字符串中最后出现的位置）
			String filePath = path + "/" + FILE_NAME;// 重新组好的配置文件路径
			if (!filePath.startsWith("/")) {// 如果不是/开头
				filePath = "/" + filePath;// 就变成/开头
			}
			File file = new File(filePath);
			FileInputStream fis = new FileInputStream(file);// 文件输入流
			BufferedInputStream bis = new BufferedInputStream(fis);// 文件输入流缓存
			Properties properties = new Properties();
			properties.load(bis);
			accessId = properties.getProperty(ACCESS_ID);
			accessKey = properties.getProperty(ACCESS_KEY);
			endpoint = properties.getProperty(ENDPOINT);
			project = properties.getProperty(PROJECT);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
