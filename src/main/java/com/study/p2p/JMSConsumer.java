package com.study.p2p;

import java.text.SimpleDateFormat;
import java.util.Date;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;

/**
 * 消息的消费者（接受者）
 * @author Administrator
 */
public class JMSConsumer {
	private static final String USERNAME = ActiveMQConnection.DEFAULT_USER;// 默认连接用户名
	private static final String PASSWORD = ActiveMQConnection.DEFAULT_PASSWORD;// 默认连接密码
	private static final String BROKEURL = ActiveMQConnection.DEFAULT_BROKER_URL;// 默认连接地址

	public static void main(String[] args) {
		ConnectionFactory connectionFactory;// 连接工厂
		Connection connection = null;// 连接
		Session session;// 会话 接受或者发送消息的线程
		Destination destination;// 消息的目的地
		MessageConsumer messageConsumer;// 消息的消费者
		// 实例化连接工厂
		connectionFactory = new ActiveMQConnectionFactory(JMSConsumer.USERNAME, JMSConsumer.PASSWORD,
				JMSConsumer.BROKEURL);
		try {
			// 通过连接工厂获取连接
			connection = connectionFactory.createConnection();
			// 启动连接
			connection.start();
			// 创建session 设置false时无事务处理，不需要commit即可发送
			// 无事务时，需要指定确认方式，一共三种
			// AUTO_ACKNOWLEDGE 自动通知  最为简单，有以下两点
			// 1、对于同步消费者,Receive方法调用返回,且没有异常发生时,将自动对收到的消息予以确认.
			// 2、对于异步消息,当onMessage方法返回,且没有异常发生时,即对收到的消息自动确认.
			// Client_AcKnowledge 客户端自行决定通知时机
			// 这种方式要求客户端使用javax.jms.Message.acknowledge()方法完成确认.
			// Dups_OK_ACKnowledge 延时、批量通知
			// 这种确认方式允许JMS不必急于确认收到的消息,允许在收到多个消息之后一次完成确认
			// 与Auto_AcKnowledge相比,这种确认方式在某些情况下可能更有效
			// 因为没有确认,当系统崩溃或者网络出现故障的时候,消息可以被重新传递. 
			session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
			// 创建一个连接HelloWorld的消息队列
			destination = session.createQueue("HelloWorldTest01");
			// 创建消息消费者
			messageConsumer = session.createConsumer(destination);
			while (true) {
				//30 秒后超时
				TextMessage textMessage = (TextMessage) messageConsumer.receive(1000 * 30); 
				if (textMessage != null) {
					//打印消息
					printMessage(textMessage);
					try {
						//创建一个新的MessageProducer来发送一个回复消息。
						MessageProducer producer = session.createProducer(textMessage.getJMSReplyTo());
						//获取名字
						String name = textMessage.getText().replace("Hello, My name is ", "");
						//创建回复消息
						TextMessage replyMessage = session.createTextMessage("Hello " + name + ", My name is Consumer");
						//不持久化消息
						producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
						//设置30秒后过期
						producer.setTimeToLive(1000 * 30);
						//设置相关的MessageId
						replyMessage.setJMSCorrelationID("HelloWorldTest01");
						//发送
						producer.send(replyMessage);
						//签收,通知broker消息已收到，在Client_AcKnowledge方式下必须调用
						textMessage.acknowledge();
					} catch (JMSException e1) {
						e1.printStackTrace();
					}
				} else {
					break;
				}
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