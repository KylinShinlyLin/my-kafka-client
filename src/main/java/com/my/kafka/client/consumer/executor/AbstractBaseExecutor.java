package com.my.kafka.client.consumer.executor;

/**
 * @program: my-kafka-client
 * @description: AbstractBaseExecutor
 * @author: ZengShiLin
 * @create: 2019-08-07 19:05
 **/
public abstract class AbstractBaseExecutor implements BaseKafkaExecutor {

    /**
     * 消费者名字
     *
     * @return 名字
     */
    public String consumerName() {
        return null;
    }
}
