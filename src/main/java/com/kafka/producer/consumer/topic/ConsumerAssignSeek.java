package com.kafka.producer.consumer.topic;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerAssignSeek {
    public static void main(String[] arg)
    {
        Logger logger= LoggerFactory.getLogger(ConsumerAssignSeek.class.getName());
        Properties properties=new Properties();
        String bootstrapserver="127.0.0.1:9092";
        String topic="first_topic";
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapserver);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String,String> consumer =new KafkaConsumer<String,String>(properties);
        TopicPartition partition= new TopicPartition(topic,0);
        long offsetToReadFrom=15L;
        consumer.assign(Arrays.asList(partition));
        consumer.seek(partition,offsetToReadFrom);
        while (true) {
            ConsumerRecords<String, String> record = consumer.poll(Duration.ofMillis(100));


            for (ConsumerRecord<String, String> records : record) {
                logger.info("KEY  " + records.key()+" Value "+records.value());
            }
        }



    }
}
