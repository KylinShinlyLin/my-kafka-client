package com.my.kafka.client.config;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Component;

/**
 * @program: finance-service
 * @description:
 * @author: ZengShiLin
 * @create: 4/6/2020 12:42 PM
 **/
@Data
@NoArgsConstructor
@AllArgsConstructor
@Component("kafkaParams")
@PropertySource(value = {"classpath*:myKafka.properties"}, ignoreResourceNotFound = true)
public class KafkaParams {

    /**
     * kafka borkers 集群,以前是zookeeper集群(从2.x版本开始kafka转变配置为broker机器IP)
     * 如果集群超过三台机器,只要配置你使用topice涉及的即可
     */
    @Value("${kafka.servers}")
    private String bootstrapServers;

    /**
     * broker收到消息回复响应级别
     * 值: [all, -1, 0, 1]
     * all:等待leader等待所有broker同步完副本回复 [all 等价于 -1]
     * 1 :leader同步完直接回复,不等待所有broker同步
     * 0 :leader不等待同步,直接响应
     */
    @Value("${kafka.acks}")
    private String acks;


    /**
     * 是否开启幂等性 true 是 false 否
     * kafka 提供三种等级的消息交付一致性
     * 最多一次 (at most once): 消息可能会丢失,但绝不不会被重复发送
     * 至少一次 (at least once): 消息不会丢失,但是可能被重复发送
     * 精确一次 (exactly once): 消息不会丢失,也不会被重复发送
     * 开启幂等性是在broker保证精确一致性(前提是kafka消息的key值必须全局唯一)
     */
    @Value("${kafka.enable.idempotence:#{null}}")
    private String enableIdempotence;

    /**
     * 项目名称
     * repair-web finance-service 等等
     */
    @Value("${kafka.app.name}")
    private String appName;


    /**
     * 是否开启事务模式
     * true 是 false 否
     */
    @Value("${kafka.enable.transactional:#{null}}")
    private String enableTransactional;


    /**
     * 使用的压缩算法
     * 默认不开启
     * 支持:gzip,snappy(facebook的高压缩率算法),lz4,zstd
     */
    @Value("${kafka.compression.type:#{null}}")
    private String compressionType;

    /**
     * 重试次数
     */
    @Value("${kafka.retries:#{null}}")
    private String retries;

    /**
     * 发送超时时间（默认120秒）
     */
    @Value("${kafka.delivery.timeout.ms:#{null}}")
    private String deliveryTimeoutMs;


    /**
     * 配置事务隔离级别（read_committed 默认 , unread_committed）
     */
    @Value("${kafka.isolation.level:#{null}}")
    private String isolationLevel;


    /**
     * 当kafka中没有初始偏移量，或者当前偏移量在服务器上不存在时(例如，因为数据已被删除)，该怎么办?
     * earliest:使用最早的偏移量
     * latest:使用最新的offset[默认,并建议使用]
     * none:抛出异常
     * anything:随机选取一个
     */
    @Value("${kafka.auto.offset.reset:#{null}}")
    private String autoOffsetReset;

    /**
     * 消费者心跳时间（默认10秒）
     */
    @Value("${kafka.session.timeout.ms:#{null}}")
    private String sessionTimeoutMs;

    /**
     * 如果开启了自动提交
     * 消费者自动提交到kafka的频率（默认5秒）
     */
    @Value("${kafka.auto.commit.interval.ms:#{null}}")
    private String autoCommitIntervalMs;

    /**
     * 消费者告警阈值
     */
    @Value("${kafka.alarm.threshold:#{null}}")
    private String alarmThreshold;

    /**
     * 在阻塞之前，客户端在单个连接上发送的未确认请求的最大数量。
     * 设置成1的时候可以严格保证消费顺序，但是发送速度会略微降低，但是影响不大
     */
    @Value("${max.in.flight.requests.per.connection:#{null}}")
    private String maxInFlightRequestsPerConnection;

    /**
     * 在阻塞之前，客户端在单个连接上发送的未确认请求的最大数量。
     * 设置成1的时候可以严格保证消费顺序，但是发送速度会略微降低，但是影响不大
     */
    @Value("${kafka.producer.num:#{null}}")
    private String producerNum;
}
