package com.my.kafka.client.consumer.executor;

import java.util.List;

/**
 * @program: my-kafka-client
 * @description: kafka消费者基础接口
 * @author: ZengShiLin
 * @create: 2019-08-07 10:22
 **/
public interface BaseKafkaExecutor {
    /**
     * 获取Topics(支持消费多个topic)
     *
     * @return topics
     */
    List<String> getTopics();

}
