package com.my.kafka.client.annotation;


import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.TYPE;

/**
 * 消费者配置注解
 *
 * @author zengshilin
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({TYPE})
public @interface MyConsumerConfig {


    /**
     * 一次消息最大拉取到数量
     *
     * @return 最大一批拉取到数量
     */
    int maxPollRecords() default 500;

    /**
     * 是否自动提交，默认是关闭的但是代码由myKafka进行提交。
     * 如果消费的时候抛出异常，不会提交消息。
     * 如果开启 autoCommit 每次消费完成后，都会被kafka-client自动提交
     *
     * @return 是/否
     */
    boolean autoCommit() default false;


    /**
     * kafka poll() 轮询等待时间，等待时间越长，拉取到的数据越多
     * 但是同时数据量也所限于，max.partition.fetch.bytes 和  max.poll.records
     *
     * @return 轮询时间
     */
    int pollMs() default 100;


    /**
     * 当kafka中没有初始偏移量，或者当前偏移量在服务器上不存在时(例如，因为数据已被删除)，该怎么办?
     * earliest:使用最早的偏移量
     * latest:使用最新的offset[默认,并建议使用]
     * none:抛出异常
     * anything:随机选取一个
     */
    String autoOffsetReset() default "latest";

    /**
     * 用于检测消费者故障超时时间
     * session.timeout.ms 会和 heartbeat.interval.ms 搭配使用
     *
     * @return 故障超时时间
     */
    int sessionTimeoutMs() default 15000;

    /**
     * heartbeat.interval.ms 心跳到组管理的预期时间
     * 如果连续三次预期内没有收到心跳，那么当前消费者会被踢出组
     * 【注意】： heartbeat.interval.ms 不要大于 session.timeout.ms 的三分之一
     * 如果大于,在 session.timeout.ms 结束时候可能服务器还没收到三次心跳
     *
     * @return 心跳预期时间
     */
    int heartbeatIntervalMs() default 3000;


    /**
     * 最大数据拉取间隔时间
     *
     * @return 拉取间隔时间
     */
    int maxPollIntervalMs() default 600000;

    /**
     * 在kafka中，为了能使多个消费者消费同一个offset的消息，需要让消费者处于不同的消费者组
     *
     * @return 消费者主后缀
     */
    String consumerGroupSuffix() default "";

    /**
     * 是否打印消费时间
     *
     * @return 是/否
     */
    boolean printConsumerTime() default false;
}
