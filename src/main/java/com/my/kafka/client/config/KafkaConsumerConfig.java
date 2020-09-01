package com.my.kafka.client.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

/**
 * 参考官网配置:http://kafka.apache.org/documentation/
 * Create by ZengShiLin on 2019-07-07
 * TODO 配置内容后续继续添加
 *
 * @author ZengShiLin
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Accessors(chain = true)
public class KafkaConsumerConfig {

    /**
     * kafka borkers 集群,以前是zookeeper集群(从2.x版本开始kafka转变配置为broker机器IP)
     * 如果集群超过三台机器,只要配置你使用topice涉及的即可
     */
    private String bootstrapServers;

    /**
     * 当kafka中没有初始偏移量，或者当前偏移量在服务器上不存在时(例如，因为数据已被删除)，该怎么办?
     * earliest:使用最早的偏移量
     * latest:使用最新的offset[默认,并建议使用]
     * none:抛出异常
     * anything:随机选取一个
     */
    private String autoOffsetReset;

    /**
     * 消费者心跳时间（默认10秒）
     */
    private String sessionTimeoutMs;

    /**
     * 如果开启了自动提交
     * 消费者自动提交到kafka的频率（默认5秒）
     */
    private String autoCommitIntervalMs;

    /**
     * 将会使用这个设置消费者组（Group ID）
     * 应用名称
     */
    private String appName;

    /**
     * 消费者告警阈值
     */
    private Long alarmThreshold;

}
