package com.demo.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemo {


    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);

        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "my_kafka_group";
        //create consumer properties
        Properties properties = new Properties();

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG , StringDeserializer.class.getName()); //converts bytes to string
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); //earliest(from beginning) OR latest(from only new message) OR none (exception)

        //Create consumer
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);


        //subscribe consumer to topics
        consumer.subscribe(Arrays.asList("java_topic"));

        while (true) {
            //poll for new data
            ConsumerRecords<String, String> consumerRecordList = consumer.poll(Duration.ofMillis(100));

            //records.forEach();

            for (ConsumerRecord<String, String> record : consumerRecordList) {
                logger.info("Key: " + record.key() +
                        " Message: " + record.value() +
                        " Partition: " + record.partition() +
                        " Offset: " + record.offset());
            }
        }

    }
}
