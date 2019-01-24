package com.qr.kafka;


import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class ProducerDemo {

	
	public static void main(String[] args) {
		Properties originalProps=new Properties();	
		//ø…≈‰÷√ƒ⁄»›µΩ πŸÕ¯
		originalProps.put("serializer.class", "kafka.serializer.StringEncoder");
		originalProps.put("metadata.broker.list", "192.168.38.131:9092,192.168.38.132:9092,192.168.38.133:9092");
        ProducerConfig config=new ProducerConfig(originalProps);
		Producer producer=new Producer<>(config);
		for(int i=0;i<100;i++)
		{
			producer.send(new KeyedMessage("topic2", "hello tom"+i));
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

}