package com.my.kafka.client.exception;

import lombok.Getter;

/**
 * @program: kafka-test
 * @description: kafka自定义异常
 * @author: ZengShiLin
 * @create: 2019-07-09 09:18
 **/
public class KafkaException extends RuntimeException {

    /**
     * 错误码
     */
    @Getter
    protected Integer code;

    /**
     * 错误信息
     */
    @Getter
    protected String msg;

    public KafkaException(Integer code, String msg) {
        super(msg);
        this.code = code;
        this.msg = msg;
    }
}
