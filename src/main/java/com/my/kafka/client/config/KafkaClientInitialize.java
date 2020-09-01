package com.my.kafka.client.config;

import com.google.common.collect.Lists;
import com.my.kafka.client.common.ConfigUtil;
import com.my.kafka.client.consumer.executor.AbstractBaseExecutor;
import com.my.kafka.client.consumer.executor.MyConsumerExecutor;
import com.my.kafka.client.monitor.MonitorExecutor;
import com.my.kafka.client.producer.MyKafkaProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.UnknownHostException;
import java.util.List;


/**
 * @program: finance-service
 * @description: 客户端初始化
 * @author: ZengShiLin
 * @create: 12/24/2019 1:54 PM
 **/
@Configuration
public class KafkaClientInitialize {

    /**
     * 所有实现消费的实例
     */
    @Autowired(required = false)
    private List<AbstractBaseExecutor> batchMessageExecutors = Lists.newArrayList();

    @Bean
    public KafkaProducerConfig producerConfig(@Autowired KafkaParams params) {
        return ConfigUtil.createProducerConfig(params);
    }

    @Bean
    public KafkaConsumerConfig consumerConfig(@Autowired KafkaParams params) {
        return ConfigUtil.createConsumerConfig(params);
    }

    @Bean
    public MyKafkaProducer kafkaProducer(@Autowired KafkaProducerConfig producerConfig) {
        return MyKafkaProducer.setConfig(producerConfig).initProducer();
    }

    @Bean
    public MyConsumerExecutor kafkaConsumer(@Autowired KafkaConsumerConfig consumerConfig) throws UnknownHostException {
        return MyConsumerExecutor.setConfig(consumerConfig, batchMessageExecutors).initConsumer();
    }


    @Bean
    public MonitorExecutor kafkaMonitor(@Autowired KafkaConsumerConfig consumerConfig) {
        return MonitorExecutor.setConfig(consumerConfig).initMonitor();
    }
}
