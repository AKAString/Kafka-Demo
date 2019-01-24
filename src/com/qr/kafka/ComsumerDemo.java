package com.qr.kafka;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
public class ComsumerDemo {

	public static void main(String[] args) {
		// kafka提供高级api
		Properties originalProps = new Properties();
		originalProps.put("group.id", "mygroup12");
		originalProps.put("auto.offset.reset", "smallest");
		originalProps.put("zookeeper.connect", ComuserConstants.ZK);
		originalProps.put("auto.commit.enable", "true");
		originalProps.put("auto.commit.interval.ms", "2000");
		originalProps.put("offsets.storage", "zookeeper");
		ConsumerConfig config = new ConsumerConfig(originalProps);
		ConsumerConnector consumer = Consumer.createJavaConsumerConnector(config);
		Map<String, Integer> topics = new HashMap<>();
		topics.put(ComuserConstants.TOPIC, 2);
		// topics.put("wc1", 2);
		Map<String, List<KafkaStream<byte[], byte[]>>> maps = consumer.createMessageStreams(topics);
		List<KafkaStream<byte[], byte[]>> kafkaStreams = maps.get(ComuserConstants.TOPIC);
		for (final KafkaStream<byte[], byte[]> stream : kafkaStreams) {	
			new Thread(){
				public void run() {
					
					for (MessageAndMetadata<byte[], byte[]> message : stream) {
						String msg = new String(message.message());
						System.err.println(msg);
					}
				}
				
			}.start();
			
		}
	}

}
