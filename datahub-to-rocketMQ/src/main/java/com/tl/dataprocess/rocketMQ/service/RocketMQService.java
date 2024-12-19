package com.tl.dataprocess.rocketMQ.service;

import com.aliyun.openservices.ons.api.SendCallback;

public interface RocketMQService {

    /**
     * 发送普通消息 (同步)
     * @param message
     * @return
     */
    Boolean sendCommonMessage(String message,String topic,String tag);

    /**
     * 发送消息 但不应答
     * @param message
     * @return
     */
    void sendOneWay(String message,String topic,String tag);

    /**
     * 发送消息 （异步）
     * @param message
     * @param sendCallback
     */
    void sendAsync(String message,String topic,String tag, SendCallback sendCallback);
}
