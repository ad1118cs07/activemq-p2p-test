package com.study.p2p;

import java.text.SimpleDateFormat;
import java.util.Calendar;
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
 * 消息的生产者（发送者）
 * 
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
		connectionFactory = new ActiveMQConnectionFactory(USERNAME, PASSWORD,
				JMSProducer.BROKEURL);
		try {
			// 通过连接工厂获取连接
			connection = connectionFactory.createConnection();
			// 启动连接
			connection.start();
			// 创建 session 设置true时有事务处理，commit后才能推送，rollback可以取消
			// 注意：事务是针对broker而不是producer的，不管session是否commit，broker都会收到message。
			session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
			// 用queueName创建的消息队列,JMSConsumer只有指定相同的queueName，才能获取消息
			destination = session.createQueue("HelloWorldTest01");
			// 创建消息生产者
			messageProducer = session.createProducer(destination);
			
			//表示一个消息的有效期。只有在这个有效期内，消息消费者才可以消费这个消息。默认值为0，表示消息永不过期。
			//下面的消息有效期将会是3分钟
			messageProducer.setTimeToLive(1000 * 60 * 3);//1000毫秒 * 60秒 * 3分
			//设置不持久化
			messageProducer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
			//消息的优先级。0-4为正常的优先级，5-9为高优先级，高优先级的消息会优先处理，可以不设置,默认为0
			messageProducer.setPriority(0);
			
			//启用Message的 JMSTimestamp , 注意:启用这个属性的时候，setTimeToLive 会失效,或许是activemq的bug
			//messageProducer.setDisableMessageTimestamp(true);
			
			// 创建需要回复的消息队列,使用临时的消息队列,仅在connection没有关闭时有效
			Destination destinationReply = session.createTemporaryQueue();
			// 也可以使用createQueue,有效期更长,但回复性的destination建议使用TemporaryQueue或TemporaryTopic
			// destinationReply = session.createQueue("replyQueue01");
			// 发送消息
			sendMessage(session, messageProducer, destinationReply);
			// 事务提交
			session.commit();
			
			// 消息的接收者
			MessageConsumer comsumerReply = session.createConsumer(destinationReply);
			while (true) {
				//30 秒后超时
				TextMessage textMessage = (TextMessage) comsumerReply.receive(1000 * 30); 
				if (textMessage != null) {
					System.out.println("receive Reply:" + textMessage.getText());
					//签收,通知broker消息已收到
					textMessage.acknowledge();
				} else {
					break;
				}
			}
			
			//另一种方式接收
//			comsumerReply.setMessageListener(new MessageListener() {
//
//				public void onMessage(Message message) {
//					try {
//						System.out.println("Reply:" + ((TextMessage) message).getText());
//					} catch (JMSException e) {
//						e.printStackTrace();
//					}
//				}
//			});
			
			System.out.println("over");
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
	 * 发送消息 
	 * @param session 
	 * @param messageProducer 消息生产者 
	 * @throws Exception
	 */
	public static void sendMessage(Session session, MessageProducer messageProducer , Destination destinationReply) throws Exception {
		for (int i = 0; i < JMSProducer.SENDNUM; i++) {
			
			//可创建的消息类型
			//TextMessage java.lang.String 对象
			//MapMessage 名/值对的集合，名是String 对象，值类型可以是Java任何基本类型
			//BytesMessage 字节流
			//StreamMessage Java 中的输入输出流
			//ObjectMessage Java 中的可序列化对象
			//Message 没有消息体，只有消息头和属性
			
			//创建一条文本消息
			TextMessage message = session.createTextMessage("Hello, My name is Producer" + i);
			//设置回复目标
			message.setJMSReplyTo(destinationReply);
			
			//setJMSTimestamp 设置消息发送的时间，Consumer可以在获取消息的时候用getJMSTimestamp读取
			//setJMSTimestamp 必须启用setDisableMessageTimestamp才能生效，但这会使setTimeToLive 无效化
			//如需要此功能，建议使用StringProperty代替
			Calendar calendar = Calendar.getInstance();
			calendar.set(2016, 1, 30);
			//如果不设置，当调用send()方法的时候，JMSTimestamp会被自动设置为当前时间
			message.setJMSTimestamp(calendar.getTimeInMillis());   //这里设置消息的时间是2016-01-30
			message.setStringProperty("time", "2016-1-30");
			
			if (i % 2 == 0) {
				//经过测试，发现此设置没用，仅在订阅者获取messsage后可以用getJMSExpiration可以查看消息的过期时间
				message.setJMSExpiration(1000000);
			}
			
			// 通过消息生产者发出消息
			messageProducer.send(message);
			
			System.out.println("send：Hello, My name is Producer" + i);
			SimpleDateFormat format = new  SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
			System.out.println("timestamp:" + format.format(new Date(calendar.getTimeInMillis())));
		}
	}
}
