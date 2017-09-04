package com.spark.bigdata.kafka;

import com.spark.bigdata.util.FileReaderUtil;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import java.util.Properties;

/**
 * crate by Sean 2017.8.24
 */
public class KafkaUtil {

    private static KafkaProducer<String,String> kp;
    private static KafkaConsumer<String, String> kc;

    private static String bootstrapServers=FileReaderUtil.getConfig("kafka","bootstrapServers");
    private static String acks=FileReaderUtil.getConfig("kafka","acks");
    private static String groupId=FileReaderUtil.getConfig("kafka","groupId");

    public static KafkaProducer<String, String> getProducer() {
        if (kp == null) {
            Properties props = new Properties();
            props.put("bootstrap.servers",bootstrapServers);
            props.put("acks", acks);
            props.put("retries", 0);
            props.put("batch.size", 16384);
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            kp = new KafkaProducer<String, String>(props);
        }
        return kp;
    }


    public static KafkaConsumer<String, String> getConsumer() {
        if(kc == null) {
            Properties props = new Properties();

            props.put("bootstrap.servers", bootstrapServers);
            props.put("group.id", groupId);
            props.put("enable.auto.commit", "true");
            /**
             * Setting enable.auto.commit means that offsets are committed automatically
             * with a frequency controlled by the config auto.commit.interval.ms.
             */
            props.put("auto.commit.interval.ms", "1000");
            props.put("session.timeout.ms", "30000");
            props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            kc = new KafkaConsumer<String, String>(props);
        }
        return kc;
    }

}
