package com.kafka.producer.consumer.topic;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerWithThread {
    public static void main(String[] arg)
    {
        new ConsumerWithThread().run();
    }
    private ConsumerWithThread()
    {

    }
    private void run()
    {
            Logger logger= LoggerFactory.getLogger(ConsumerWithThread.class.getName());
            String bootstrapserver = "127.0.0.1:9092";
            String topic = "first_topic";
            String GroupId = "my-application";
            CountDownLatch latch = new CountDownLatch(1);
            Runnable myconsumerthread = new ConsumerThread(latch
                    , bootstrapserver,GroupId,
                    topic);
            Thread mythread= new Thread(myconsumerthread);
            mythread.start();
            Runtime.getRuntime().addShutdownHook(new Thread(()->
            {
                logger.info("Caught shutdown hook");
                ((ConsumerThread) myconsumerthread).shutdown();
            }
            ));
        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.info("Error message",e);
        }

        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        finally {
            logger.info("Application shut down");
        }
    }

}
class  ConsumerThread implements  Runnable
{
    private CountDownLatch latch;
    private KafkaConsumer<String,String> consumer;

    private Logger logger= LoggerFactory.getLogger(ConsumerThread.class.getName());
    public  ConsumerThread(CountDownLatch latch,String bootstrapserver,String GroupId,String topic)
    {
        this.latch=latch;

        Properties properties=new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapserver);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GroupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        consumer =new KafkaConsumer<String,String>(properties);
        consumer.subscribe(Arrays.asList(topic));
    }
    @Override
    public void run()
    {
        try
        {
        while (true)
            {
            ConsumerRecords<String, String> record = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> records : record)
            {
                logger.info("KEY  " + records.key()+" Value "+records.value());
            }
            }
        }
        catch (WakeupException e)
        {
            logger.info("Wake up Exception");
        }
        finally
        {
            consumer.close();
            latch.countDown();
        }

    }
    public void shutdown()
    {
        consumer.wakeup();
    }
}
