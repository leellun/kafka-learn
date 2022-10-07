package com.newland.kafka.producer;

import com.newland.kafka.constant.KafkaConstant;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * 异步发送
 * Author: leell
 * Date: 2022/10/7 16:52:35
 */
public class TransactionProducer {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties properties=new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConstant.BOOTSTRAP_SERVERS);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG,"transaction01");

        KafkaProducer<String,String> kafkaProducer=new KafkaProducer<String, String>(properties);

        kafkaProducer.initTransactions();
        kafkaProducer.beginTransaction();

        try{
            kafkaProducer.send(new ProducerRecord<>("first","事务消息"));
            kafkaProducer.commitTransaction();
        }catch (Exception e){
            kafkaProducer.abortTransaction();
        }finally {
            kafkaProducer.close();
        }
    }
}
