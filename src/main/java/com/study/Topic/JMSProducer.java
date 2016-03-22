package com.study.Topic;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;

/**
 * 消息的生产者（发送者）
 * @author Administrator
 *
 */
public class JMSProducer {
	// 默认连接用户名
	private static final String USERNAME = ActiveMQConnection.DEFAULT_USER; // 默认连接密码
	private static final String PASSWORD = ActiveMQConnection.DEFAULT_PASSWORD; // 默认连接地址
	private static final String BROKEURL = ActiveMQConnection.DEFAULT_BROKER_URL; // 发送的消息数量
	private static final int SENDNUM = 2;

	public static void main(String[] args) {
		// 连接工厂
		ConnectionFactory connectionFactory;
		// 连接
		Connection connection = null;
		// 会话 接受或者发送消息的线程
		Session session;
		// 消息的目的地
		Destination destination;
		// 消息生产者
		MessageProducer messageProducer;
		// 实例化连接工厂
		connectionFactory = new ActiveMQConnectionFactory(JMSProducer.USERNAME, JMSProducer.PASSWORD,
				JMSProducer.BROKEURL);
		try {
			// 通过连接工厂获取连接
			connection = connectionFactory.createConnection();
			// 启动连接
			connection.start();
			// 创建  session
			session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
			// 创建一个Topic
			destination = session.createTopic("TestTopic02");
			// 创建消息生产者
			messageProducer = session.createProducer(destination);
			//持久化消息
			messageProducer.setDeliveryMode(DeliveryMode.PERSISTENT);
			//表示一个消息的有效期。只有在这个有效期内，消息消费者才可以消费这个消息。默认值为0，表示消息永不过期。
			messageProducer.setTimeToLive(1000 * 60 * 5); //1000毫秒 * 60秒 * 5分
			//消息的优先级。0-4为正常的优先级，5-9为高优先级
			messageProducer.setPriority(9);
			
			// 发送消息
			sendMessage(session, messageProducer);
			session.commit();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (connection != null) {
				try {
					connection.close();
				} catch (JMSException e) {
					e.printStackTrace();
				}
			}
		}
	}

	/**
	 * * 发送消息 
	 * @param session 
	 * @param messageProducer 消息生产者 
	 * @throws Exception
	 */
	public static void sendMessage(Session session, MessageProducer messageProducer) throws Exception {
		for (int i = 0; i < JMSProducer.SENDNUM; i++) { 
			// 创建一条文本消息 
			TextMessage message = session.createTextMessage("Topic消息" +i);
			//message.setJMSExpiration(expiration);
			System.out.println("发送消息：Activemq 发送Topic消息" + i);
			//通过消息生产者发出消息
			messageProducer.send(message);
		}
	}
}
