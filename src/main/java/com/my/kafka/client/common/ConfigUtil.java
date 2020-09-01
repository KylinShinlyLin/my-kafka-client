package com.my.kafka.client.common;

import com.my.kafka.client.annotation.MyConsumerConfig;
import com.my.kafka.client.common.enums.PartitionAssignorEnum;
import com.my.kafka.client.config.KafkaConsumerConfig;
import com.my.kafka.client.config.KafkaParams;
import com.my.kafka.client.config.KafkaProducerConfig;
import com.my.kafka.client.consumer.TopicConsumerGroup;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.*;

/**
 * @program: finance-service
 * @description:
 * @author: ZengShiLin
 * @create: 12/25/2019 2:31 PM
 **/
public class ConfigUtil {


    /**
     * 获取消费者配置
     *
     * @param consumerConfig 消费者配置
     * @return 消费者配置
     */
    public static Properties getConsumerPropertie(KafkaConsumerConfig consumerConfig, MyConsumerConfig annotations, TopicConsumerGroup topicConsumerGroup) {
        Properties properties = new Properties();
        boolean annotationsNotNull = Objects.nonNull(annotations);
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, consumerConfig.getBootstrapServers());
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, annotationsNotNull && annotations.autoCommit());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, topicConsumerGroup.getConsumerGroup());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, annotationsNotNull ? annotations.autoOffsetReset() :
                Optional.ofNullable(consumerConfig.getAutoOffsetReset()).orElse("latest"));
        properties.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, annotationsNotNull ? annotations.heartbeatIntervalMs() : 3000);
        properties.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, annotationsNotNull ? annotations.maxPollIntervalMs() : 300000);
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, annotationsNotNull ? annotations.sessionTimeoutMs() : "15000");
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, annotationsNotNull ? annotations.maxPollRecords() : 100);
        properties.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 52428800);
        properties.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, 52428800);
        properties.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, true);
        //确保名称在当前JVM里面是唯一的
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, topicConsumerGroup.getConsumerName() + "_" + topicConsumerGroup.getIp());
        //Rebalance先写死，后面重新做这块的配
        properties.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, Collections.singletonList(PartitionAssignorEnum.Range.getAssignor()));
        return properties;
    }


    /**
     * 配置内容(使用protected)
     *
     * @return 配置内容
     */
    public static Properties getProducerproperties(KafkaProducerConfig producerConfig) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.ACKS_CONFIG, Optional.ofNullable(producerConfig.getAcks()).orElse("all"));
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, producerConfig.getBootstrapServers());
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, producerConfig.isEnableIdempotence());
        //严格保证消费顺序
        properties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
        if (producerConfig.isEnableTransactional()) {
            properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, producerConfig.getAppName() + "_TRANSACTIONAL_ID_" + UUID.randomUUID().toString());
            properties.put("isolation.level", Optional.ofNullable(producerConfig.getIsolationLevel()).orElse("read_committed"));
        }
        //client ID
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, producerConfig.getAppName().toUpperCase() + "_CLIENT_ID_" + UUID.randomUUID().toString());
        return properties;
    }

    /**
     * 创建生产者配置
     *
     * @param params 参数
     * @return 生产者配置
     */
    public static KafkaProducerConfig createProducerConfig(KafkaParams params) {
        return KafkaProducerConfig.builder()
                .bootstrapServers(params.getBootstrapServers())
                .acks(params.getAcks())
                .enableIdempotence(Optional.ofNullable(params.getEnableIdempotence()).map(Boolean::parseBoolean).orElse(Boolean.FALSE))
                .appName(params.getAppName())
                .enableTransactional(Optional.ofNullable(params.getEnableTransactional()).map(Boolean::parseBoolean).orElse(Boolean.FALSE))
                .compressionType(params.getCompressionType())
                .retries(params.getRetries())
                .deliveryTimeoutMs(params.getDeliveryTimeoutMs())
                .isolationLevel(params.getIsolationLevel())
                .maxInFlightRequestsPerConnection(Optional.ofNullable(params.getMaxInFlightRequestsPerConnection()).map(Integer::new).orElse(5))
                .producerNum(Optional.ofNullable(params.getProducerNum()).map(Integer::new).orElse(1))
                .build();
    }

    /**
     * 创建消费者配置
     *
     * @param params 参数
     * @return 消费者配置
     */
    public static KafkaConsumerConfig createConsumerConfig(KafkaParams params) {
        return KafkaConsumerConfig.builder()
                .bootstrapServers(params.getBootstrapServers())
                .autoOffsetReset(params.getAutoOffsetReset())
                .sessionTimeoutMs(params.getSessionTimeoutMs())
                .autoCommitIntervalMs(params.getAutoCommitIntervalMs())
                .appName(params.getAppName())
                .alarmThreshold(Optional.ofNullable(params.getAlarmThreshold()).map(Long::new).orElse(null))
                .build();
    }
}
