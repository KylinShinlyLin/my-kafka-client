package com.my.kafka.client.monitor;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;

/**
 * @program: finance-service
 * @description:
 * @author: ZengShiLin
 * @create: 12/24/2019 4:31 PM
 **/
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Accessors(chain = true)
public class OffsetWrap {


    /**
     * broker端的 offset
     */
    private Map<TopicPartition, OffsetAndMetadata> consumedOffsets;

    /**
     * 消费者组的 offset
     */
    private Map<TopicPartition, Long> endOffsers;

}
