package com.datasource;

import com.huaweicloud.dis.DIS;
import com.huaweicloud.dis.DISClientBuilder;

import java.io.*;
import java.net.URL;
import java.util.Properties;

public class InitDis {
    public static DIS dis = null;

    private void initDis() {
        // 创建DIS客户端实例
        dis = getInstance();

    }

    public static DIS getInstance() {
        if (dis == null) {
            synchronized (InitDis.class) {
                if (dis == null) {
                    URL url = InitDis.class.getClassLoader().getResource("config.properties");
                    File file = new File(url.getPath());
                    Properties properties = new Properties();
                    try {
                        FileReader fileReader = new FileReader(file);
                        properties.load(fileReader);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    String point = properties.getProperty("point");
                    String ak = properties.getProperty("ak");
                    String sk = properties.getProperty("sk");
                    String projectId = properties.getProperty("projectId");
                    String region = properties.getProperty("region");
                    dis = DISClientBuilder.standard()
                            .withEndpoint(point)
                            .withAk(ak)
                            .withSk(sk)
                            .withProjectId(projectId)
                            .withRegion(region)
                            .build();
                }
            }
        }
        return dis;
    }
}
