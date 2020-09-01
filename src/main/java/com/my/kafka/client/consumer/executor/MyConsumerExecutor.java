package com.my.kafka.client.consumer.executor;

import com.google.common.collect.Lists;
import com.my.kafka.client.annotation.MyConsumerConfig;
import com.my.kafka.client.common.CommonConstant;
import com.my.kafka.client.config.KafkaConsumerConfig;
import com.my.kafka.client.consumer.TopicConsumerGroup;
import com.my.kafka.client.exception.KafkaException;
import com.my.kafka.client.exception.KafkaExceptionEnum;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.springframework.util.CollectionUtils;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;


/**
 * @author 资金流
 */
@Slf4j
public class MyConsumerExecutor {


    private static volatile MyConsumerExecutor instance;

    /**
     * 消费者全局配置
     */
    private static KafkaConsumerConfig globalConsumerConfig;

    /**
     * 批量消费者
     */
    private static List<AbstractBaseExecutor> abstractBaseExecutors = Lists.newArrayList();

    /**
     * 当前正在运行的线程
     * <Topic,Runnable>
     */
    @Getter
    private static final ConcurrentHashMap<TopicConsumerGroup, ConsumerRunnable> runnableMaps = new ConcurrentHashMap<>();

    /**
     * 是否已经初始化（volatile 增强可见性）
     */
    private volatile static boolean INITIALIZE = false;

    /**
     * 使用线程池统一管理(消费者线程池)
     */
    @Getter
    private static ThreadPoolExecutor consumerPoolExecutor;


    public static MyConsumerExecutor setConfig(KafkaConsumerConfig consumerConfig, List<AbstractBaseExecutor> executors) {
        synchronized (MyConsumerExecutor.class) {
            globalConsumerConfig = consumerConfig;
            abstractBaseExecutors = executors;
            return null == instance ? (instance = new MyConsumerExecutor()) : instance;
        }
    }

    private MyConsumerExecutor() {
    }

    /**
     * 消费者关闭（记得提醒运维信号量建议使用 kill 15）
     * 不然钩子很可能不会被触发
     * synchronized
     */
    private synchronized void shutdown() {
        runnableMaps.values().forEach(e -> e.getIsRun().set(false));
        INITIALIZE = false;
        log.info("kafka消费者关闭");
    }

    /**
     * 消费者初始化
     * synchronized
     */
    public synchronized MyConsumerExecutor initConsumer() throws UnknownHostException {
        log.info(CommonConstant.INFO);
        if (INITIALIZE) {
            log.info("consumer has inited");
            return instance;
        }
        if (null == globalConsumerConfig) {
            throw new KafkaException(KafkaExceptionEnum.CONSUMER_CONFIGURATION_IS_EMPTY.getCode()
                    , KafkaExceptionEnum.CONSUMER_CONFIGURATION_IS_EMPTY.getDescription());
        }
        if (CollectionUtils.isEmpty(abstractBaseExecutors)) {
            log.info("empty Executor instance");
            return instance;
        }
        InetAddress address = InetAddress.getLocalHost();
        String ip = address.getHostAddress();
        //group By topic + consumerGroup
        Map<TopicConsumerGroup, List<AbstractBaseExecutor>> batchExecutorMap = abstractBaseExecutors.stream()
                .collect(Collectors.groupingBy(e -> new TopicConsumerGroup(
                        e.getTopics(),
                        globalConsumerConfig.getAppName(),
                        Optional.ofNullable(e.getClass())
                                .map(e1 -> e1.getAnnotation(MyConsumerConfig.class))
                                .map(MyConsumerConfig::consumerGroupSuffix)
                                .orElse(StringUtils.EMPTY),
                        StringUtils.isBlank(e.consumerName()) ?
                                Objects.requireNonNull(e.getClass()).getSimpleName() : e.consumerName(), ip)));
        int size = batchExecutorMap.size();
        //消费者线程池
        consumerPoolExecutor = new ThreadPoolExecutor(
                size,
                //预留一个线程给consumer重启使用
                size + 1,
                1000L,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(8),
                new BasicThreadFactory.Builder()
                        .namingPattern("kafka-consumer-thread-pool")
                        .build()
        );
        batchExecutorMap.forEach((key, value) -> runnableMaps.put(key,
                new ConsumerRunnable(globalConsumerConfig, key, value)));
        runnableMaps.values().forEach(e -> consumerPoolExecutor.execute(e));
        INITIALIZE = true;
        Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown));
        return instance;
    }


}
