package com.study.p2p;

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

public class JMSSelectorTest {
	private static final String USERNAME = ActiveMQConnection.DEFAULT_USER; // 默认连接密码
	private static final String PASSWORD = ActiveMQConnection.DEFAULT_PASSWORD; // 默认连接地址
	private static final String BROKEURL = ActiveMQConnection.DEFAULT_BROKER_URL; // 发送的消息数量
	public static void main(String[] args) throws Exception {
		//创建工厂
		ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(USERNAME,
				PASSWORD, BROKEURL);
		//创建连接
		Connection connection = connectionFactory.createConnection();
		//打开连接
		connection.start();
		//创建消息队列
		Queue queue = new ActiveMQQueue("testQueue");
		//创建 session
		Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

		//Selector 类似于SQL-92的一种语法,可以用来比较消息头信息和属性
		//创建消息消费者A,取Message的属性receiver的值是A的消息
		MessageConsumer comsumerA = session.createConsumer(queue, "receiver = 'A'");
		comsumerA.setMessageListener(new MessageListener() {
			public void onMessage(Message m) {
				try {
					System.out.println("ConsumerA get " + ((TextMessage) m).getText());
				} catch (JMSException e1) {
				}
			}
		});
		//创建消息消费者B,取Message的属性receiver的值是B的消息
		MessageConsumer comsumerB = session.createConsumer(queue, "receiver = 'B'");
		comsumerB.setMessageListener(new MessageListener() {
			public void onMessage(Message m) {
				try {
					System.out.println("ConsumerB get " + ((TextMessage) m).getText());
				} catch (JMSException e) {
				}
			}
		});
		//创建消息生成者，设置Message的StringProperty,key是receiver,值是i能整除3的是B,否则是A
		MessageProducer producer = session.createProducer(queue);
		for (int i = 0; i < 3; i++) {
			String receiver = (i % 3 == 0 ? "A" : "B");
			TextMessage message = session.createTextMessage("Message" + i + ", receiver:" + receiver);
			message.setStringProperty("receiver", receiver);
			producer.send(message);
		}
	}
}
