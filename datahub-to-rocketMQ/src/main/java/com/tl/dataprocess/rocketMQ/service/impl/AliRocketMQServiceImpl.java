package com.tl.dataprocess.rocketMQ.service.impl;

import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.aliyun.openservices.ons.api.Message;
import com.aliyun.openservices.ons.api.OnExceptionContext;
import com.aliyun.openservices.ons.api.SendCallback;
import com.aliyun.openservices.ons.api.SendResult;
import com.aliyun.openservices.ons.api.bean.ProducerBean;
import com.aliyun.openservices.ons.api.exception.ONSClientException;
import com.tl.dataprocess.rocketMQ.configuration.properties.MqConfig;
import com.tl.dataprocess.rocketMQ.service.RocketMQService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

/**
 * @author ：chenguanxing
 * @date ：Created in 2022/7/7 14:18
 * @description ：
 * @version: 1.0.0.0
 */
@Service("aliRocketMQService")
public class AliRocketMQServiceImpl implements RocketMQService {

    private static final Logger logger = LoggerFactory.getLogger(AliRocketMQServiceImpl.class);

    @Autowired
    private MqConfig mqConfig;

    private ProducerBean producer;

    @PostConstruct
    public void init()
    {
        if(null == producer){
            //创建一个生产者
            this.producer = new ProducerBean();
            this.producer.setProperties(mqConfig.getMqPropertie());
        }
        //启动生产者
        this.producer.start();
    }

    /**
     * 发送普通消息 同步
     * @param message
     * @return
     */
    @Override
    public Boolean sendCommonMessage(String message,String topic,String tag) {
        Message msg = new Message( //
                // Message所属的Topic
                topic,
                // Message Tag 可理解为Gmail中的标签，对消息进行再归类，方便Consumer指定过滤条件在MQ服务器过滤
                tag,
                // Message Body 可以是任何二进制形式的数据， MQ不做任何干预
                // 需要Producer与Consumer协商好一致的序列化和反序列化方式
                message.getBytes());
        // 设置代表消息的业务关键属性，请尽可能全局唯一
        // 以方便您在无法正常收到消息情况下，可通过MQ 控制台查询消息并补发
        // 注意：不设置也不会影响消息正常收发
//        msg.setKey("ORDERID_100");
        // 发送消息，只要不抛异常就是成功
        Boolean flag = true;
        try {
            SendResult sendResult = producer.send(msg);
            assert sendResult != null;
            logger.info("----- send the msg -----"+sendResult.toString());
        } catch (ONSClientException e) {
            flag = false;
            e.printStackTrace();
            logger.info("=======send failed ======"+e.getMessage());
            //出现异常意味着发送失败，为了避免消息丢失，建议缓存该消息然后进行重试。
        }
        return flag;
    }

    @Override
    public void sendOneWay(String message,String topic,String tag) {
        Message msg = new Message(
                // Message所属的Topic
                topic,
                // Message Tag 可理解为Gmail中的标签，对消息进行再归类，方便Consumer指定过滤条件在MQ服务器过滤
                tag,
                // Message Body
                // 任何二进制形式的数据，消息队列RocketMQ版不做任何干预，需要Producer与Consumer协商好一致的序列化和反序列化方式。
                message.getBytes());

        // 设置代表消息的业务关键属性，请尽可能全局唯一。
        // 以方便您在无法正常收到消息情况下，可通过消息队列RocketMQ版控制台查询消息并补发。
        // 注意：不设置也不会影响消息正常收发。
//        msg.setKey("ORDERID_" + i);

        // 由于在oneway方式发送消息时没有请求应答处理，如果出现消息发送失败，则会因为没有重试而导致数据丢失。若数据不可丢，建议选用可靠同步或可靠异步发送方式。
        producer.sendOneway(msg);
    }

    @Override
    public void sendAsync(String message,String topic,String tag, SendCallback sendCallback) {
        Message msg = new Message( //
                // Message所属的Topic
                topic,
                // Message Tag 可理解为Gmail中的标签，对消息进行再归类，方便Consumer指定过滤条件在MQ服务器过滤
                tag,
                // Message Body 可以是任何二进制形式的数据， MQ不做任何干预
                // 需要Producer与Consumer协商好一致的序列化和反序列化方式
                message.getBytes());
        // 设置代表消息的业务关键属性，请尽可能全局唯一
        // 以方便您在无法正常收到消息情况下，可通过MQ 控制台查询消息并补发
        // 注意：不设置也不会影响消息正常收发
//        msg.setKey("ORDERID_100");

        // 发送消息，只要不抛异常就是成功
        try {
            producer.sendAsync(msg, sendCallback);
        } catch (ONSClientException e) {
            e.printStackTrace();
            logger.info("=======send failed ======"+e.getMessage());
        }
    }

    @PreDestroy
    public void shutdown() {
        this.producer.shutdown();
        System.out.println("Shutdown producer service");
    }
}
