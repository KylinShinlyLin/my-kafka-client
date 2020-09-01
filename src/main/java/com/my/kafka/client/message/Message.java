package com.my.kafka.client.message;

import com.alibaba.fastjson.JSON;
import lombok.Getter;
import org.apache.kafka.common.record.TimestampType;

import java.io.Serializable;
import java.util.Optional;
import java.util.UUID;

/**
 * @program: kafka-test
 * @description: kafka 消息体(生产者使用) 因为有 final 修饰所以使用不了 builder
 * @author: ZengShiLin
 * @create: 2019-07-09 12:36
 **/
public class Message<T> implements Serializable {

    /**
     * 默认值
     */
    public static final int NULL = -1;
    public static final long NULL_LONG = -1;

    /**
     * 需要发送的topic(创建后就不允许修改)
     */
    @Getter
    private final String topic;

    /**
     * 消息的key值(创建后就不允许修改)
     */
    @Getter
    private final String key;

    /**
     * 消息的value值(创建后就不允许修改)
     */
    @Getter
    private final T value;

    /**
     * 消息的offset
     */
    @Getter
    private final Long offset;


    /**
     * 需要指定的 partition
     */
    @Getter
    private final Integer partition;

    /**
     * 当前分区Leader选举次数，用来判断kafka当前消息版本
     */
    @Getter
    private final Optional<Integer> leaderEpoch;

    /**
     * 时间戳
     */
    @Getter
    private final Long timestamp;

    /**
     * 时间类型
     */
    private final TimestampType timestampType;


    /**
     * 构造函数一
     *
     * @param topic 消息指定topic
     * @param value 消息值
     */
    public Message(String topic, T value) {
        this(UUID.randomUUID().toString(), topic, value, NULL, NULL_LONG, Optional.empty(), System.currentTimeMillis(), TimestampType.CREATE_TIME);
    }


    /**
     * 构造函数二
     *
     * @param key   消息key
     * @param topic 消息指定topic
     * @param value 消息值
     */
    public Message(String key, String topic, T value) {
        this(key, topic, value, NULL, NULL_LONG, Optional.empty(), System.currentTimeMillis(), TimestampType.CREATE_TIME);
    }

    /**
     * 构造函数三
     *
     * @param key       消息key
     * @param topic     消息指定topic
     * @param value     消息值
     * @param partition 消息发送指定 partition
     */
    public Message(String key, String topic, T value, Integer partition) {
        this(key, topic, value, partition, NULL_LONG, Optional.empty(), System.currentTimeMillis(), TimestampType.CREATE_TIME);
    }

    /**
     * 构造函数四
     *
     * @param key       消息key
     * @param topic     消息指定topic
     * @param value     消息值
     * @param timestamp 时间戳
     * @param partition 消息发送指定 partition
     */
    public Message(String key, String topic, T value, Long timestamp, Integer partition) {
        this(key, topic, value, partition, NULL_LONG, Optional.empty(), timestamp, TimestampType.CREATE_TIME);
    }

    /**
     * 构造函数五
     *
     * @param key       消息key
     * @param topic     消息topic
     * @param value     消息值
     * @param partition 消息对应的 partition
     * @param offset    消息的 offset
     */
    public Message(String key, String topic, T value, Integer partition, Long offset) {
        this(key, topic, value, partition, offset, Optional.empty(), System.currentTimeMillis(), TimestampType.CREATE_TIME);
    }


    public Message(String key, String topic, T value, Integer partition,
                   Long offset, Optional<Integer> leaderEpoch, Long timestamp, TimestampType timestampType) {
        this.key = key;
        this.value = value;
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.leaderEpoch = leaderEpoch;
        this.timestamp = timestamp;
        this.timestampType = timestampType;
    }

    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }
}
