package com.my.kafka.client.monitor;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.my.kafka.client.common.CommonConstant;
import com.my.kafka.client.config.KafkaConsumerConfig;
import com.my.kafka.client.consumer.TopicConsumerGroup;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.util.CollectionUtils;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

/**
 * @program: finance-service
 * @description: kafka Topic offset 监控
 * @author: ZengShiLin
 * @create: 12/24/2019 4:30 PM
 **/
@Slf4j
public class OffsetMonitor implements Monitor<Map<TopicConsumerGroup, Map<TopicPartition, Long>>> {


    /**
     * kafka消费者配置
     */
    private static KafkaConsumerConfig consumerConfig;

    /**
     * 消费者组
     */
    private static Set<TopicConsumerGroup> consumerGroups;

    /**
     * 用来告警的函数接口
     */
    @Getter
    private static final List<BiFunction<TopicPartition, Long, Integer>> kafkaAlarmFn = Lists.newArrayList();


    public OffsetMonitor(KafkaConsumerConfig config, Set<TopicConsumerGroup> consumerGroupSet) {
        consumerConfig = config;
        consumerGroups = consumerGroupSet;
    }


    private OffsetMonitor() {

    }

    /**
     * 获取消费者组的 lag
     *
     * @param groupId         消费者组ID
     * @param bootstrapServer broker集群IP
     * @return key<TopicPartition, 消息offset差距>
     */
    public Map<TopicPartition, Long> lagOf(String groupId, String bootstrapServer) {
        OffsetWrap offsetWrap = this.getConsumerGroupAndBrokerOffset(groupId, bootstrapServer);
        if (null == offsetWrap) {
            return Maps.newHashMap();
        }
        Map<TopicPartition, OffsetAndMetadata> consumedOffsets = offsetWrap.getConsumedOffsets();
        Map<TopicPartition, Long> endOffsers = offsetWrap.getEndOffsers();
        return endOffsers.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,
                //当前consumer组的HW  - 当前消费者组的offset = 消息消费差距
                entry -> entry.getValue() - consumedOffsets.get(entry.getKey()).offset()));
    }


    /**
     * 查询当前消费者组的Consumer offset 和 broker 上面的offset
     *
     * @param groupId         消费者组ID
     * @param bootstrapServer broker集群IP
     */
    public OffsetWrap getConsumerGroupAndBrokerOffset(String groupId, String bootstrapServer) {
        Properties props = new Properties();
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        try (AdminClient client = AdminClient.create(props)) {
            ListConsumerGroupOffsetsResult result = client.listConsumerGroupOffsets(groupId);
            try {
                Map<TopicPartition, OffsetAndMetadata> consumedOffsets = result.partitionsToOffsetAndMetadata().get(1, TimeUnit.MINUTES);
                props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
                props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
                props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
                props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
                try (final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
                    //gain offset
                    Map<TopicPartition, Long> endOffsers = consumer.endOffsets(consumedOffsets.keySet());
                    return OffsetWrap.builder()
                            .consumedOffsets(consumedOffsets)
                            .endOffsers(endOffsers)
                            .build();
                } catch (Exception e) {
                    log.error("Create KafkaConsumer failed");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.error("InterruptedException:", e);
            } catch (ExecutionException e) {
                log.error("ExecutionException:", e);
            } catch (TimeoutException e) {
                log.error("TimeoutException:", e);
            }
            return null;
        } catch (Exception e) {
            return null;
        }
    }


    @Override
    public Map<TopicConsumerGroup, Map<TopicPartition, Long>> execute() {
        Map<TopicConsumerGroup, Map<TopicPartition, Long>> resultMap = Maps.newHashMap();
        for (TopicConsumerGroup group : consumerGroups) {
            Map<TopicPartition, Long> topicPartitionLongMap = lagOf(consumerConfig.getAppName().toUpperCase(), consumerConfig.getBootstrapServers());
            CollectionUtils.isEmpty(topicPartitionLongMap);
            topicPartitionLongMap.forEach((key, value) -> {
                if (value > Optional.ofNullable(consumerConfig.getAlarmThreshold())
                        .orElse(CommonConstant.ALARM_THRESHOLD)) {
                    log.error("Occur Messages are stacked,Topic:{},Partition:{},lagSize：{}", key.topic(), key.partition(), value);
                    //通过函数式接口告警
                    if (!CollectionUtils.isEmpty(OffsetMonitor.getKafkaAlarmFn())) {
                        for (BiFunction<TopicPartition, Long, Integer> fn : OffsetMonitor.getKafkaAlarmFn()) {
                            fn.apply(new TopicPartition(key.topic(), key.partition()), value);
                        }
                    }
                } else {
                    log.info("Topic:{},Partition:{},lagSize：{}", key.topic(), key.partition(), value);
                }
            });
            resultMap.put(group, topicPartitionLongMap);
        }
        return resultMap;
    }
}
