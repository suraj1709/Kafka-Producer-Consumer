package com.kafka.producer.consumer.topic;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerWithKey {
    public static void main(String[] args) {

        final Logger logger = LoggerFactory.getLogger(ProducerWithKey.class);
        Properties properties = new Properties();
        String bootstrapserver = "127.0.0.1:9092";
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapserver);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        for (int i = 0; i < 10; i++) {
            KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", Integer.toString(i),"hello world" +Integer.toString(i));

            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        logger.info("Topic Name : " + recordMetadata.topic() + "Partion :" + recordMetadata.partition() + "Offset : " + recordMetadata.offset() +
                                "TimeStamp: " + recordMetadata.timestamp());
                    } else {
                        logger.error("Error ", e);
                    }
                }
            });
            producer.flush();
        }
    }
}

