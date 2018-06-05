package com.chatRobot.mqtt;

import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.io.IOException;
import java.util.Date;

/**
 * @Author: yangliang
 * @Date: Created in 10:01 2018/6/1
 * @Desription:
 **/
public class MQTTSendMsg {

    public static void main(String[] args) throws IOException {

        final String broker ="tcp://localhost:1883";
        //final String broker ="tcp://172.0.0.1:8161";

        final String acessKey ="admin";
        final String secretKey ="admin";
        final String topic ="topic1";
        final String clientId ="GID_topic1@@@ClientID_2";
        String sign;
        MemoryPersistence persistence = new MemoryPersistence();

        try {
            final MqttClient sampleClient = new MqttClient(broker, clientId, persistence);
            final MqttConnectOptions connOpts = new MqttConnectOptions();

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
                }
                public void connectionLost(Throwable throwable) {
                    System.out.println("mqtt connection lost");
                }
                public void messageArrived(String topic, MqttMessage mqttMessage) throws Exception {
                    System.out.println("messageArrived:" + topic + "------" + new String(mqttMessage.getPayload()));
                }
                public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {
                    System.out.println("deliveryComplete:" + iMqttDeliveryToken.getMessageId());
                }
            });
            sampleClient.connect(connOpts);

            for (int i = 0; i < 10; i++) {
                try {
                    String scontent = new Date()+"MQTT Test body"  + i;
                    //此处消息体只需要传入 byte 数组即可，对于其他类型的消息，请自行完成二进制数据的转换
                    final MqttMessage message = new MqttMessage(scontent.getBytes());
                    message.setQos(0);
                    sampleClient.publish(topic, message);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        } catch (Exception me) {
            me.printStackTrace();
        }
    }

}
