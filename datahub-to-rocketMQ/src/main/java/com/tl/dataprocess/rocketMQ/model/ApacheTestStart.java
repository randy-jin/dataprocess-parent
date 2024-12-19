//package com.tl.dataprocess.rocketMQ.model;
//
//import org.apache.rocketmq.client.producer.DefaultMQProducer;
//import org.apache.rocketmq.client.producer.SendResult;
//import org.apache.rocketmq.common.message.Message;
//
///**
// * @author ：chenguanxing
// * @date ：Created in 2022/7/5 17:14
// * @description ：
// * @version: 1.0.0.0
// */
//public class ApacheTestStart {
//
//    public static void main(String[] args) throws Exception {
//        // 1 创建消息生产者，指定生成组名
//        DefaultMQProducer defaultMQProducer = new DefaultMQProducer("jack-producer-group");
//        // 2 指定NameServer的地址
//        defaultMQProducer.setNamesrvAddr("localhost:9876");
//        // 3 启动生产者
//        defaultMQProducer.start();
//        // 4 构建消息对象，主要是设置消息的主题、标签、内容 结合业务
//        /**
//         * 业务 模拟发送动⽕作业类⽬标
//         */
//        Message message = new Message("emergency_event", "jack-tag", ("{ \"objType\":\"acetylenecylinder\", \"objLeft\":1408, \"objRight\":1271, \"objTop\":406, \"objBottom\":539, \"cameraId\":\"11010200011320700031\", \"timestamp\":1559193126478, \"entryTime\":1559193126478, \"leaveTime\":1559193126478, \"feature\":\"\", \"label\":\"1\", \"isDHPC\":\"0\", \"origImage\":\"http://xxx.com/origImage/20190530/11010200011320700031/155919322 7789.jpg\", \"cropImage\":\"http://xxx.com/cropImage/20190530/11010200011320700031/155919322 8245.jpg\" }").getBytes());
//
//        // 5 发送消息
//        SendResult result = defaultMQProducer.send(message);
//        System.out.println("SendResult-->" + result);
//        // 6 关闭生产者
//        defaultMQProducer.shutdown();
//    }
//}
