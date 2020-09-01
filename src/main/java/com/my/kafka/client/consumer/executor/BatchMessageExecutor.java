package com.my.kafka.client.consumer.executor;

import com.my.kafka.client.message.Message;

import java.util.List;

/**
 * @program: my-kafka-client
 * @description: 批量消费消息并且能自行提交的接口
 * @author: ZengShiLin
 * @create: 2019-08-07 10:23
 **/
public abstract class BatchMessageExecutor extends AbstractBaseExecutor {

    /**
     * 批量消息消费接口
     * <消息key,消息value>
     *
     * @param messages 消费者
     */
    public abstract void execute(List<Message<String>> messages) throws Exception;

}
