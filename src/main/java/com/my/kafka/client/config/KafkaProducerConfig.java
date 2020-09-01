package com.my.kafka.client.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

/**
 * 参考官网配置:http://kafka.apache.org/documentation/
 * Create by ZengShiLin on 2019-07-07
 * 生产者配置
 * TODO 后续拓展更多
 *
 * @author ZengShiLin
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Accessors(chain = true)
public class KafkaProducerConfig {

    /**
     * kafka borkers 集群,以前是zookeeper集群(从2.x版本开始kafka转变配置为broker机器IP)
     * 如果集群超过三台机器,只要配置你使用topice涉及的即可
     */
    private String bootstrapServers;

    /**
     * broker收到消息回复响应级别
     * 值: [all, -1, 0, 1]
     * all:等待leader等待所有broker同步完副本回复 [all 等价于 -1]
     * 1 :leader同步完直接回复,不等待所有broker同步
     * 0 :leader不等待同步,直接响应
     */
    private String acks;


    /**
     * 是否开启幂等性 true 是 false 否
     * kafka 提供三种等级的消息交付一致性
     * 最多一次 (at most once): 消息可能会丢失,但绝不不会被重复发送
     * 至少一次 (at least once): 消息不会丢失,但是可能被重复发送
     * 精确一次 (exactly once): 消息不会丢失,也不会被重复发送
     * 开启幂等性是在broker保证精确一致性(前提是kafka消息的key值必须全局唯一)
     */
    private boolean enableIdempotence;

    /**
     * 项目名称
     * repair-web finance-service 等等
     */
    private String appName;


    /**
     * 是否开启事务模式
     * true 是 false 否
     */
    private boolean enableTransactional;


    /**
     * 使用的压缩算法
     * 默认不开启
     * 支持:gzip,snappy(facebook的高压缩率算法),lz4,zstd
     */
    private String compressionType;

    /**
     * 重试次数
     */
    private String retries;

    /**
     * 发送超时时间（默认120秒）
     */
    private String deliveryTimeoutMs;


    /**
     * 配置事务隔离级别（read_committed 默认 , unread_committed）
     */
    private String isolationLevel;


    /**
     * 在阻塞之前，客户端在单个连接上发送的未确认请求的最大数量。
     * 设置成1的时候可以严格保证消费顺序，但是发送速度会略微降低，但是影响不大
     */
    private Integer maxInFlightRequestsPerConnection;

    /**
     * 生产者数量
     */
    private Integer producerNum;
}
