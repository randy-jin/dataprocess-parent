package com.ls.athena.utils;

import cn.hutool.http.HttpRequest;
import cn.hutool.http.HttpResponse;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

//@RestController
public class Monitor {

    @Value("${monitor.path}")
    private String path;

    @Value("${monitor.port}")
    private int port;

    @RequestMapping(value = "/Monitor", method = RequestMethod.GET)
    public String mon() {
        HttpRequest httpRequest = new HttpRequest("localhost:" + port + "/" + path);
        HttpResponse execute = httpRequest.execute();
        return execute.body();
    }
}
