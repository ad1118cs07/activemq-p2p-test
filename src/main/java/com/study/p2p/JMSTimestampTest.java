package com.study.p2p;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQQueue;

public class JMSTimestampTest {
	// 默认连接用户名
	private static final String USERNAME = ActiveMQConnection.DEFAULT_USER; // 默认连接密码
	private static final String PASSWORD = ActiveMQConnection.DEFAULT_PASSWORD; // 默认连接地址
	private static final String BROKEURL = ActiveMQConnection.DEFAULT_BROKER_URL; // 发送的消息数量
	
	public static void main(String[] args)  throws Exception {
		//创建工厂
		ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(USERNAME,
				PASSWORD, BROKEURL);
		//创建连接
		Connection connection = connectionFactory.createConnection();
		//打开连接
		connection.start();
		//创建消息队列
		Queue queue = new ActiveMQQueue("testTimestamp");

		Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

		//创建消息消费者
		MessageConsumer comsumer = session.createConsumer(queue);
		//监听消息
		comsumer.setMessageListener(new MessageListener() {
			public void onMessage(Message m) {
				try {
					System.out.println("get: " + ((TextMessage) m).getText());
					SimpleDateFormat format = new  SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
					//获取消息发送的时间
					System.out.println("timestamp:" + format.format(new Date(m.getJMSTimestamp())));
				} catch (JMSException e1) {
				}
			}
		});
		
		//创建消息生成者
		MessageProducer producer = session.createProducer(queue);
		
		//启用Message的 JMSTimestamp , 注意:启用这个属性的时候，setTimeToLive 会失效,或许是activemq的bug
		producer.setDisableMessageTimestamp(true);
		//创建消息
		TextMessage message = session.createTextMessage("Message Test");
		//setJMSTimestamp 设置消息发送的时间，Consumer可以在获取消息的时候用getJMSTimestamp读取
		//setJMSTimestamp 必须启用setDisableMessageTimestamp才能生效，但这会使setTimeToLive 无效化
		//如需要此功能，建议使用StringProperty代替
		Calendar calendar = Calendar.getInstance();
		calendar.set(2016, 1, 30);
		//如果不设置，当调用send()方法的时候，JMSTimestamp会被自动设置为当前时间
		message.setJMSTimestamp(calendar.getTimeInMillis());   //这里设置消息的时间是2016-01-30
		
		producer.send(message);
	}

}
