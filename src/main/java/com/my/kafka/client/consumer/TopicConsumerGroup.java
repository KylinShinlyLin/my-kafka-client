package com.my.kafka.client.consumer;

import lombok.AccessLevel;
import lombok.Data;
import lombok.experimental.FieldDefaults;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

/**
 * @program: finance-service
 * @description:
 * @author: ZengShiLin
 * @create: 4/16/2020 12:04 PM
 **/
@FieldDefaults(level = AccessLevel.PRIVATE)
@Data
public class TopicConsumerGroup {

    /**
     * 对应的topic
     */
    List<String> topics;

    /**
     * 对应的consumerGroup
     */
    String consumerGroup;

    /**
     * 消费者名称
     */
    String consumerName;

    /**
     * 机器IP
     */
    String ip;

    public TopicConsumerGroup(List<String> topics, String appName, String consumerGroupSuffix, String consumerName, String ip) {
        this.topics = topics;
        this.consumerGroup = StringUtils.isBlank(consumerGroupSuffix) ? appName.toUpperCase()
                : appName.toUpperCase() + "-" + consumerGroupSuffix;
        this.consumerName = consumerName;
        this.ip = ip;
    }
}
