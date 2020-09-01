package com.my.kafka.client.consumer.executor;

import com.my.kafka.client.message.OldMessage;

import java.util.List;

/**
 * @program: finance-service
 * @description: 将推广新版消息实体，兼容版本的消费者会逐渐废弃
 * @author: ZengShiLin
 * @create: 4/6/2020 10:54 AM
 **/
@Deprecated
public abstract class BatchForOldMessageExecutor extends AbstractBaseExecutor {

    /**
     * 批量消息消费接口
     * <消息key,消息value>
     *
     * @param messages 消费者
     */
    public abstract void execute(List<OldMessage> messages) throws Exception;
}
