package com.newland.kafka.customer;

import com.newland.kafka.constant.KafkaConstant;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Set;

/**
 * offset寻址
 * Author: leell
 * Date: 2022/10/7 16:58:31
 */
public class ConsumerSeek {
    public static void main(String[] args) {
        // 0 配置
        Properties properties = new Properties();

        // 连接 bootstrap.servers
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConstant.BOOTSTRAP_SERVERS);

        // 反序列化
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // 配置消费者组id
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test4");

        // 设置分区分配策略
        properties.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, "org.apache.kafka.clients.consumer.StickyAssignor");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);

        ArrayList<String> topics = new ArrayList<>();
        topics.add("first");
        kafkaConsumer.subscribe(topics);

        Set<TopicPartition> assigment = kafkaConsumer.assignment();
        while (assigment.size() == 0) {
            kafkaConsumer.poll(Duration.ofSeconds(1));
            assigment = kafkaConsumer.assignment();
        }
        for (TopicPartition topicPartition : assigment) {
            kafkaConsumer.seek(topicPartition, 1);
        }
        while (true) {
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> record : consumerRecords) {
                System.out.println(record);
            }
        }
    }
}
