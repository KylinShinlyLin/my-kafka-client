package com.my.kafka.client.consumer.executor;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.Lists;
import com.my.kafka.client.annotation.MyConsumerConfig;
import com.my.kafka.client.common.ConfigUtil;
import com.my.kafka.client.config.KafkaConsumerConfig;
import com.my.kafka.client.consumer.TopicConsumerGroup;
import com.my.kafka.client.exception.KafkaExceptionEnum;
import com.my.kafka.client.message.Message;
import com.my.kafka.client.message.OldMessage;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.springframework.util.StopWatch;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * @program: finance-service
 * @description: 消费运行线程
 * @author: ZengShiLin
 * @create: 12/24/2019 11:27 AM
 **/
@Slf4j
public class ConsumerRunnable implements Runnable {

    private final KafkaConsumerConfig globalConsumerConfig;


    private final List<AbstractBaseExecutor> executors;

    @Getter
    private final TopicConsumerGroup topicConsumerGroup;


    @Getter
    private final AtomicBoolean isRun = new AtomicBoolean(true);

    @Getter
    private KafkaConsumer<String, String> consumer;


    ConsumerRunnable(
            KafkaConsumerConfig globalConsumerConfig,
            TopicConsumerGroup topicConsumerGroup,
            List<AbstractBaseExecutor> executors) {
        this.globalConsumerConfig = globalConsumerConfig;
        this.topicConsumerGroup = topicConsumerGroup;
        this.executors = executors;
    }

    @Override
    public void run() {
        Thread.currentThread().setName(String.format("kafka-consumer-thread-%s-group-%s"
                , topicConsumerGroup.getConsumerName(), topicConsumerGroup.getConsumerGroup()));
        log.info(String.format("init consumer,ConsumerName:%s,consumerGroup:%s"
                , this.topicConsumerGroup.getConsumerName(), this.topicConsumerGroup.getConsumerGroup()));
        MyConsumerConfig annotationsConfig = executors.get(0).getClass().getAnnotation(MyConsumerConfig.class);
        int pollMs = Objects.nonNull(annotationsConfig) ? annotationsConfig.pollMs() : 100;
        boolean needPrintTime = Objects.nonNull(annotationsConfig) && annotationsConfig.printConsumerTime();
        Properties properties = this.setConfig(globalConsumerConfig, annotationsConfig, topicConsumerGroup);
        try {
            this.consumer = new KafkaConsumer<>(properties);
        } catch (Exception e) {
            e.printStackTrace();
            log.error(String.format(KafkaExceptionEnum.CONSUMER_INITIALIZE_FAIL.getDescription()
                    , topicConsumerGroup.getConsumerName(), e.getMessage()));
            return;
        }
        //只消费指定的Topic
        this.consumer.subscribe(this.topicConsumerGroup.getTopics(), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                log.warn(String.format("ConsumerName:%s,ConsumerGroup:%s[start rebalance]partitions:%s"
                        , topicConsumerGroup.getConsumerName(), topicConsumerGroup.getConsumerGroup(), JSON.toJSONString(partitions)));
                consumer.commitSync();
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                log.warn(String.format("ConsumerName:%s,ConsumerGroup:%s[end rebalance]partitions:%s"
                        , topicConsumerGroup.getConsumerName(), topicConsumerGroup.getConsumerGroup(), JSON.toJSONString(partitions)));
            }
        });
        StopWatch stopWatch;
        List<Message<String>> messages;
        ConsumerRecords<String, String> records;
        boolean success = false;
        try {
            while (isRun.get()) {
                try {
                    stopWatch = new StopWatch(String.format("ConsumerName:%s,ConsumerGroup:%s"
                            , this.topicConsumerGroup.getConsumerName(), this.topicConsumerGroup.getConsumerGroup()));
                    stopWatch.start(String.format("Pull Message %s", topicConsumerGroup.getConsumerName()));
                    records = this.consumer.poll(Duration.ofMillis(pollMs));
                    stopWatch.stop();
                    if (records.isEmpty()) {
                        continue;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    log.error(String.format(KafkaExceptionEnum.POLL_MESSAGE_FAIL.getDescription(), topicConsumerGroup.getConsumerName(), e.getMessage()));
                    continue;
                }
                try {
                    messages = this.transformMessage(records);
                    stopWatch.start(String.format("Consumer Message,Size:%s", messages.size()));
                    success = this.batchConsumer(messages);
                    stopWatch.stop();
                } catch (Exception | Error e) {
                    if (stopWatch.isRunning()) {
                        stopWatch.stop();
                    }
                    log.error(String.format("consumer failed,ConsumerName:%s \n%s", topicConsumerGroup.getConsumerName(), stopWatch.shortSummary()), e);
                    success = false;
                } finally {
                    try {
                        if (success) {
                            if (stopWatch.isRunning()) {
                                stopWatch.stop();
                            }
                            stopWatch.start("commit offset");
                            this.consumer.commitSync();
                            stopWatch.stop();
                        }
                        if (needPrintTime) {
                            log.info(stopWatch.prettyPrint());
                        }
                    } catch (Exception e) {
                        if (stopWatch.isRunning()) {
                            stopWatch.stop();
                        }
                        log.error(String.format(KafkaExceptionEnum.CONSUMER_COMMIT_FAIL.getDescription()
                                , topicConsumerGroup.getConsumerName(), e.getMessage(), stopWatch.prettyPrint()));
                    }
                }

            }
        } finally {
            //关闭消费者
            this.consumer.close();
            log.info(String.format("ConsumerName:%s Closed", topicConsumerGroup.getConsumerName()));
        }
    }

    /**
     * 消费者配置
     *
     * @param globalConsumerConfig 全局配置
     * @param annotationsConfig    注解配置
     * @return 返回消费者配置
     */
    private Properties setConfig(KafkaConsumerConfig globalConsumerConfig, MyConsumerConfig annotationsConfig, TopicConsumerGroup topicConsumerGroup) {
        return ConfigUtil.getConsumerPropertie(globalConsumerConfig, annotationsConfig, topicConsumerGroup);
    }

    /**
     * ConsumerRecords 是迭代器 , 这里装换成集合返回
     *
     * @param records records
     * @return records 集合
     */
    private List<Message<String>> transformMessage(ConsumerRecords<String, String> records) {
        return Lists.newArrayList(records).stream()
                .map(record -> new Message<>(record.key(), record.topic(), record.value(), record.partition(), record.offset(),
                        record.leaderEpoch(), record.timestamp(), record.timestampType()))
                .collect(Collectors.toList());
    }


    /**
     * 批量消费
     *
     * @param messages 消息记录
     */
    @Deprecated
    private boolean batchConsumer(List<Message<String>> messages) throws Exception {
        for (AbstractBaseExecutor executor : this.executors) {
            //TODO 为了兼容旧版本，后续会逐渐替换下线
            if (executor instanceof BatchForOldMessageExecutor) {
                ((BatchForOldMessageExecutor) executor).execute(messages.stream()
                        .map(Message::getValue)
                        .map(e -> JSON.parseObject(e, OldMessage.class))
                        .collect(Collectors.toList()));
            } else {
                ((BatchMessageExecutor) executor).execute(messages);
            }
        }
        return true;
    }


    public boolean isThreadActive() {
        return Thread.currentThread().isAlive();
    }


    public boolean isRun() {
        return isRun.get();
    }

}