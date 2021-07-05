package com.intel.devcloud.gen2.consumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;


public class KafkaConsumerController {
	
	public static void getMessage(String bootstrapServer) throws Exception {
		System.out.println("Inside KafkaConsumerController|bootstrapServer="+bootstrapServer);
		Properties properties = new Properties();
		properties.put("bootstrap.servers", bootstrapServer);
		properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		properties.put("group.id", "test_group");
		//properties.put("security.protocol", "SSL");
		//properties.put("ssl.truststore.location", "/deployments/keystore.jks");
		//properties.put("ssl.truststore.password", "password");
		/*properties.put("max.partition.fetch.bytes", 10485760);
        properties.put("enable.auto.commit", "true");
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("session.timeout.ms", "30000");*/

		KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);
		kafkaConsumer.subscribe(Collections.singletonList("celeron"));
		boolean process = Boolean.FALSE;
		System.out.println("************ kafka Consumer **********");
		try{
			while (true){
				process = Boolean.FALSE;
				Duration timeout = Duration.ofMillis(1);
				ConsumerRecords<String, String> records = kafkaConsumer.poll(timeout);
				long startTime = System.nanoTime();
				//System.out.println("********* Polling queue for messages....");
				for (ConsumerRecord<String, String> record: records){
					System.out.println("******** Message retrieved from the queue");
					//System.out.println("***********************************************************");
					System.out.println("***********************************************************"+String.format("Topic - %s, Partition - %d, Value: %s", record.topic(), record.partition(), record.value()));
					process = Boolean.TRUE;
				}
				if(!process)continue;
				long endTime = System.nanoTime();
				System.out.println("*********************************************************** Total time taken = "+ (endTime-startTime)/1000000 +" milliseconds.");
			}
		}catch (Exception e){
			System.out.println(e.getMessage());
		}finally {
			kafkaConsumer.close();
		}
	}

}
