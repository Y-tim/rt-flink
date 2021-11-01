package com.atguigu.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import javax.xml.bind.annotation.XmlType;
import java.util.Properties;

/**
 * @author Logan
 * @create 2021-07-28 14:39
 */
public class MyKafkaUtil {
    private static String kafkaserver="hadoop102:9092,hadoop103:9092,hadoop104:9092";
    private static String defaultTopic = "dwd_fact";
    public static FlinkKafkaConsumer<String> getFlinkKafkaCounsumer(String topic,String groupId){
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,kafkaserver);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        return new FlinkKafkaConsumer<String>(topic,new SimpleStringSchema(), properties);
    }
    public static FlinkKafkaProducer<String> getFlinkafkaProducer(String topic){
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,kafkaserver);
        return new FlinkKafkaProducer<String>(topic,new SimpleStringSchema(),properties);
    }
    //不理解
    public static <T> FlinkKafkaProducer<T> getFlinkafkaProducer(KafkaSerializationSchema<T> kafkaSerializationSchema){
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,kafkaserver);
        properties.setProperty(ProducerConfig.ACKS_CONFIG,"1");
        return new FlinkKafkaProducer<T>(
                defaultTopic,
                kafkaSerializationSchema,
                properties,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        );

        }
    //拼接Kafka相关属性到DDL
    public static String getKafkaDDL(String topic, String groupId) {
        return "  WITH ('connector' = 'kafka', " +
                " 'topic' = '" + topic + "'," +
                " 'properties.bootstrap.servers' = '" + kafkaserver + "', " +
                " 'properties.group.id' = '" + groupId + "', " +
                " 'format' = 'json', " +
                " 'scan.startup.mode' = 'latest-offset')";
    }
}
