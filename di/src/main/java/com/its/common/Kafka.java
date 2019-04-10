package com.its.common;
import java.io.IOException;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
public class Kafka {
	static Properties properties=null;
	static Tools tools=new Tools();
	
	static{
        properties = new Properties();
        try {
			properties.load(Kafka.class.getClassLoader().getResourceAsStream("kafka.properties"));
		} catch (IOException e) {
			e.printStackTrace();
			
		}
	}	
	public static Producer<String, String> createProducer(Properties properties){
		KafkaProducer<String, String> producer = null;
        try {
            producer = new KafkaProducer<String, String>(properties);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return producer;
	}
	public static KafkaConsumer<String,String> createConsumer(String groupid){
		//properties.put("group.id", groupid);
		properties.put(ConsumerConfig.GROUP_ID_CONFIG,groupid);
		KafkaConsumer<String,String> consumer=new KafkaConsumer<>(properties);	
		return consumer;
	}
    public static void insertToKafka(String topic,StringBuffer strbuffer){
    	Producer<String, String> producer=null;
		producer = createProducer(properties);
		producer.send(new ProducerRecord<String, String>(topic,strbuffer.toString()));
		producer.flush();
		producer.close();    	
    }    
	public static void main(String [] args) throws IOException{
		//insertToKafka("testtopic1",new StringBuffer("bbbb"));
	}

}
