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
 * 持久订阅消费者（接受者）
 * 测试时可以先运行JMSConsumer2,这时会注册clientID,关闭运行
 * 再运行JMSProducer,发送消费
 * 在消息的有效期内,重新启动JMSConsumer2,此时会发现可以收到消息
 * @author Administrator
 */
public class JMSConsumer2 {
	private static final String USERNAME = ActiveMQConnection.DEFAULT_USER;// 默认连接用户名
	private static final String PASSWORD = ActiveMQConnection.DEFAULT_PASSWORD;// 默认连接密码
	private static final String BROKEURL = ActiveMQConnection.DEFAULT_BROKER_URL;// 默认连接地址

	public static void main(String[] args) {
		ConnectionFactory connectionFactory;// 连接工厂
		Connection connection = null;// 连接
		Session session;// 会话 接受或者发送消息的线程
		// 实例化连接工厂
		connectionFactory = new ActiveMQConnectionFactory(JMSConsumer2.USERNAME, JMSConsumer2.PASSWORD,
				JMSConsumer2.BROKEURL);
		try {
			// 通过连接工厂获取连接
			connection = connectionFactory.createConnection();
			
			//持久订阅需要设置ClientID
			connection.setClientID("TopicConsumer2"); 
			
			// 启动连接
			connection.start();
			// 创建session 设置false时无事务处理，不需要commit即可发送,使用自行决定通知
			session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
			// 创建一个Topic
			Topic topic = session.createTopic("TestTopic02");
			// 创建一个持久订阅
			MessageConsumer messageConsumer = session.createDurableSubscriber(topic,"TopicSubscriber2");
			
			
			//50 秒后超时
			TextMessage textMessage = (TextMessage) messageConsumer.receive(1000 * 50); 
			if (textMessage != null) {
				//打印消息
				printMessage(textMessage);
				//签收,通知broker消息已收到，在Client_AcKnowledge方式下必须调用
				textMessage.acknowledge();
			}
			
//			// 使用事件模式接收
//			messageConsumer.setMessageListener(new MessageListener() {
//				
//				public void onMessage(Message message) {
//					try {
//						//打印消息
//						if (message instanceof TextMessage) {
//							printMessage((TextMessage)message);
//						}
//						//签收,通知broker消息已收到，在Client_AcKnowledge方式下必须调用
//						message.acknowledge();
//					} catch (JMSException e) {
//						e.printStackTrace();
//					}
//				}
//			});

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