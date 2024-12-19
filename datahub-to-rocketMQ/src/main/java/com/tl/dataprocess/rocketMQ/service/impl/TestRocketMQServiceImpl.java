package com.tl.dataprocess.rocketMQ.service.impl;

import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.remoting.exception.RemotingException;
import com.aliyun.openservices.ons.api.SendCallback;
import com.tl.dataprocess.rocketMQ.service.RocketMQService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

/**
 * @author ：chenguanxing
 * @date ：Created in 2022/7/6 14:03
 * @description ：社区版TCP协议的SDK;可以直接测试外网MQ
 * @version: 1.0.0.0
 */
@Service("testRocketMQService")
public class TestRocketMQServiceImpl implements RocketMQService {

    private static final Logger logger = LoggerFactory.getLogger(TestRocketMQServiceImpl.class);

    @Value("${rocketmq.test.producerGroup}")
    private String producerGroup;

    @Value("${rocketmq.test.namesrvAddr}")
    private String namesrvAddr;

    @Value("${rocketmq.test.instanceName}")
    private String instanceName;

//    @Value("${rocketmq.test.topic}")
//    private String topic;
//
//    @Value("${rocketmq.test.tag}")
//    private String tag;

    DefaultMQProducer producer;

    @PostConstruct
    public void init()
    {
        if(null == producer){
            //创建一个生产者
            this.producer=new DefaultMQProducer(producerGroup);
            //设置NameServer地址
            this.producer.setNamesrvAddr(namesrvAddr);
            //设置生产者实例名称
            this.producer.setInstanceName(instanceName);
        }
        //启动生产者
        try {
            this.producer.start();
        } catch (MQClientException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Boolean sendCommonMessage(String msg,String topic,String tag) {

        Boolean flag = true;
        try {
            //创建消息，topic主题名称  tags临时值代表小分类， body代表消息体
            Message message = new Message(topic,tag,msg.getBytes());
            //发送消息
            SendResult sendResult = this.producer.send(message);
            logger.info("------send the msgId -----"+sendResult.getMsgId());
        } catch (Exception e) {
            flag = false;
            e.printStackTrace();
        }
        return flag;
    }

    /**
     * 发送普通消息 但不应答
     * @param msg
     * @return
     */
    @Override
    public void sendOneWay(String msg,String topic,String tag) {
        Message message = new Message(topic,tag,msg.getBytes());
        //发送消息
        try {
            this.producer.sendOneway(message);
        } catch (MQClientException e) {
            e.printStackTrace();
        } catch (RemotingException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        logger.info("=======send over ======");
    }

    @Override
    public void sendAsync(String msg,String topic,String tag, SendCallback sendCallback) {
        Message message = new Message(topic,tag,msg.getBytes());
        try {
            producer.send(message, new com.alibaba.rocketmq.client.producer.SendCallback() {
                @Override public void onSuccess(SendResult result) {
                    // 消费发送成功。
                    System.out.println("send message success. msgId= " + result.getMsgId());
                }

                @Override public void onException(Throwable throwable) {
                    // 消息发送失败，需要进行重试处理，可重新发送这条消息或持久化这条数据进行补偿处理。
                    System.out.println("send message failed.");
                    throwable.printStackTrace();
                }
            });
        } catch (Exception e) {
            // 消息发送失败，需要进行重试处理，可重新发送这条消息或持久化这条数据进行补偿处理。
            e.printStackTrace();
        }
    }

    @PreDestroy
    public void shutdown() {
        this.producer.shutdown();
        System.out.println("Shutdown producer service");
    }

}
