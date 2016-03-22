package com.study.Topic;

import java.text.SimpleDateFormat;
import java.util.Date;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;

/**
 * 普通订阅者（接受者）
 * @author Administrator
 */
public class JMSConsumer1 {
	private static final String USERNAME = ActiveMQConnection.DEFAULT_USER;// 默认连接用户名
	private static final String PASSWORD = ActiveMQConnection.DEFAULT_PASSWORD;// 默认连接密码
	private static final String BROKEURL = ActiveMQConnection.DEFAULT_BROKER_URL;// 默认连接地址

	public static void main(String[] args) {
		ConnectionFactory connectionFactory;// 连接工厂
		Connection connection = null;// 连接
		Session session;// 会话 接受或者发送消息的线程
		// 实例化连接工厂
		connectionFactory = new ActiveMQConnectionFactory(JMSConsumer1.USERNAME, JMSConsumer1.PASSWORD,
				JMSConsumer1.BROKEURL);
		try {
			// 通过连接工厂获取连接
			connection = connectionFactory.createConnection();
			// 启动连接
			connection.start();
			// 创建session 设置false时无事务处理，不需要commit即可发送,使用自行决定通知
			session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
			// 创建一个Topic
			Topic topic = session.createTopic("TestTopic02");
			// 创建一个普通订阅
			MessageConsumer messageConsumer = session.createConsumer(topic);
			
			//50 秒后超时
			TextMessage textMessage = (TextMessage) messageConsumer.receive(1000 * 50); 
			if (textMessage != null) {
				//打印消息
				printMessage(textMessage);
				//签收,通知broker消息已收到，在Client_AcKnowledge方式下必须调用
				textMessage.acknowledge();
			}
		} catch (JMSException e) {
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
		System.out.println("finish");
	}
	
	/**
	 * 打印消息
	 * @param textMessage
	 * @throws JMSException
	 */
	private static void printMessage(TextMessage textMessage) throws JMSException {
		SimpleDateFormat format = new  SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
		
		//打印接收到的text
		System.out.println("receive:" + textMessage.getText());
		//输出message的发送时间
		System.out.println("timestamp:" + format.format(new Date(textMessage.getJMSTimestamp())));
		//输出message的time属性值
		System.out.println("time:" + textMessage.getStringProperty("time"));
		//输出message的过期时间, 等于 Destination 的send 方法中的timeToLive 值加上发送时刻的GMT 时间值
		//仅可以用setTimeToLive设置,不能设置setJMSExpiration
		if (textMessage.getJMSExpiration() != 0) {
			System.out.println("expiration:" + format.format(new Date(textMessage.getJMSExpiration())));
		}
	}
}