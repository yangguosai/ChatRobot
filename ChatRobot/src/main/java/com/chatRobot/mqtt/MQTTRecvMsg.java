package com.chatRobot.mqtt;


import org.eclipse.paho.client.mqttv3.*;

import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.io.IOException;

/**
 * @Author: yangliang
 * @Date: Created in 10:02 2018/6/1
 * @Desription:
 **/
public class MQTTRecvMsg {

    public static void main(String[] args) throws IOException {

        final String broker ="tcp://localhost:1883";
        final String acessKey ="admin";
        final String secretKey ="admin";
        final String topic ="topic1";
        final String clientId ="GID_topic1@@@ClientID_1";
        MemoryPersistence persistence = new MemoryPersistence();




        try {
            final MqttClient sampleClient = new MqttClient(broker, clientId, persistence);
            final MqttConnectOptions connOpts = new MqttConnectOptions();

            final String[] topicFilters=new String[]{topic};
            final int[]qos={0};
            connOpts.setUserName(acessKey);
            connOpts.setServerURIs(new String[] { broker });
            connOpts.setPassword(secretKey.toCharArray());
            connOpts.setCleanSession(true);
            connOpts.setKeepAliveInterval(90);
            connOpts.setAutomaticReconnect(true);

            sampleClient.setCallback(new MqttCallbackExtended() {
                public void connectComplete(boolean reconnect, String serverURI) {
                    System.out.println("connect success");
                    //连接成功，需要上传客户端所有的订阅关系
                    try {
                        sampleClient.subscribe(topicFilters,qos);
                    } catch (MqttException e) {
                        e.printStackTrace();
                    }
                }
                public void connectionLost(Throwable throwable) {
                    System.out.println("mqtt connection lost");
                }
                public void messageArrived(String topic, MqttMessage mqttMessage) throws Exception {
                    //System.out.println("messageArrived:" + topic + "------" + new String(mqttMessage.getPayload()));
                    System.out.println("messageArrived:" + topic + "------" + new String(mqttMessage.getPayload()));
                }
                public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {
                    System.out.println("deliveryComplete:" + iMqttDeliveryToken.getMessageId());
                }
            });
            //客户端每次上线都必须上传自己所有涉及的订阅关系，否则可能会导致消息接收延迟
            sampleClient.connect(connOpts);
            //每个客户端最多允许存在30个订阅关系，超出限制可能会丢弃导致收不到部分消息
            sampleClient.subscribe(topicFilters,qos);
            Thread.sleep(Integer.MAX_VALUE);
        } catch (Exception me) {
            me.printStackTrace();
        }
    }
}
