package com.my.kafka.client.monitor;

import com.my.kafka.client.config.KafkaConsumerConfig;
import com.my.kafka.client.consumer.executor.MyConsumerExecutor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;

import java.util.Objects;
import java.util.function.BiFunction;


/**
 * @program: my-kafka-client
 * @description: kafka消费者监控
 * 为什么要监控消费者？
 * 1、如果消息堆积过多会导致消费缓慢，也可能是消息丢失
 * 2、当消息滞后到一定程度的时候，kafka的零拷贝技术会失去效果
 * 3、消息积压及时告警有助于增加partition和consumer去提升吞吐量
 * TODO 可能不考虑放在这里（可以考虑抽离到监控服务项目里面）
 * @author: ZengShiLin
 * @create: 2019-07-24 09:38
 **/
@Slf4j
public class MonitorExecutor {


    private static volatile MonitorExecutor instance;

    /**
     * kafka消费者配置
     */
    private static KafkaConsumerConfig consumerConfig;

    /**
     * 告警阈值（默认阈值）
     */
    private static Long ALARM_THRESHOLD = 500L;

    /**
     * offset监控
     */
    @Getter
    private static OffsetMonitor offsetMonitor;

    /**
     * 线程监控
     */
    @Getter
    private static ThreadMonitor threadMonitor;

    /**
     * 用来告警的函数式接口(客户端可以通过自己的方式去实现告警)
     *
     * @param fn 函数
     */
    public static void addAlarmInterface(BiFunction<TopicPartition, Long, Integer> fn) {
        if (Objects.nonNull(fn)) {
            OffsetMonitor.getKafkaAlarmFn().add(fn);
        }
    }


    public static MonitorExecutor setConfig(KafkaConsumerConfig config) {
        synchronized (MonitorExecutor.class) {
            consumerConfig = config;
            return null == instance ? (instance = new MonitorExecutor()) : instance;
        }
    }

    /**
     * 定时监控
     */

    public MonitorExecutor initMonitor() {
        log.info("初始化kafka客户端监控");
        offsetMonitor = new OffsetMonitor(consumerConfig, MyConsumerExecutor.getRunnableMaps().keySet());
        threadMonitor = new ThreadMonitor();
        return instance;
    }


}
