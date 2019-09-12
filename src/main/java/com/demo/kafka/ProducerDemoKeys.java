package com.demo.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);

        String bootstrapServers = "127.0.0.1:9092";
        //create producer properties
        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG , StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG , StringSerializer.class.getName());


        //create produceR
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for (int i = 0; i < 10; i++) {

            String topic = "java_topic";
            String key = "id_" +Integer.toString(i);
            String message = "Hello From Producer Demo With Callback. #" + Integer.toString(i);


            //create producer record - same keys will go to to same partition
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, message);

            logger.info("Key: " + key); //log key - Its observed all keys go to same partition i.e Partition 0, 1, 2 on re-running multiple times

            //send data - synchronous
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    //executes everytime a record is successfully sent or exception is thrown

                    if (e == null) {
                        //record was sucessfully sent
                        logger.info("Recieved new metadata. \n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp() + "\n");
                    } else {
                        logger.error("Error while producing: ", e);
                    }
                }
            }).get(); //block .send() to make synchronous - dont do in production
        }
        //flush data
        producer.flush();

        //flush and close producer
        producer.close();
    }
}
