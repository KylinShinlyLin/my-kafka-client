package com.my.kafka.client.exception;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * kafka 异常枚举
 *
 * @author ZengShiLin
 */
@AllArgsConstructor
public enum KafkaExceptionEnum {

    /**
     * 异常错误码和异常内容
     */
    PRODUCER_DON_NOT_INIT(10000, "生产者还未初始化"),
    PRODUCER_THREADLOCAL_INITIALIZE_FAILURE(10001, "ThreadLocal生产者初始化失败"),
    PRODUCER_NOT_ASSIGN_TOPIC(10002, "生成者发送失败，没有指定Topic"),
    PRODUCER_SEND_FAILURE(10004, "生产者发送消息失败"),
    PRODUCER_OPEN_TRANSACTION_FAILURE(10005, "开启事务失败,message:%s"),
    CONSUMER_CONFIGURATION_IS_EMPTY(10007, "消费者配置为空"),
    CONSUMER_INITIALIZE_FAIL(10008, "消费者初始化失败,Topic:%s,message:%s"),
    PRODUCER_INITIALIZE_FAIL(10010, "生产者初始化失败,message:%s"),
    POLL_MESSAGE_FAIL(10013, "拉取消息异常,Topic:%s,message:%s"),
    CONSUMER_COMMIT_FAIL(10014, "消费者提交异常,Topic:%s,message:%s \n%s");
    /**
     * 枚举value
     */
    @Getter
    private int code;
    /**
     * 枚举名称
     */
    @Getter
    private String description;


}
