package com.tl.dataprocess.rocketMQ.model;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * @author ：chenguanxing
 * @date ：Created in 2022/7/5 16:05
 * @description ：
 * @version: 1.0.0.0
 */
public class AliTestClientCunsumer {

    public static void main(String[] args) throws MQClientException {
        //创建消费者
        DefaultMQPushConsumer consumer=new DefaultMQPushConsumer("rmq-group");
        //设置NameServer地址
        consumer.setNamesrvAddr("localhost:9876");
        //设置实例名称
        consumer.setInstanceName("consumer");
        //订阅topic
        consumer.subscribe("test%cms_tg_outage","TagA");

        //监听消息
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                //获取消息
                for (MessageExt messageExt:list){
                    //RocketMQ由于是集群环境，所有产生的消息ID可能会重复
                    System.out.println(messageExt.getMsgId()+"----message 是  ---"+new String(messageExt.getBody()));
                }
                //接受消息状态 1.消费成功    2.消费失败   队列还有
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        //启动消费者
        consumer.start();
        System.out.println("consumer Started!");
    }
}
