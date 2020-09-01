package com.my.kafka.client.common.enums;

import lombok.Getter;
import org.apache.kafka.clients.consumer.RangeAssignor;
import org.apache.kafka.clients.consumer.RoundRobinAssignor;
import org.apache.kafka.clients.consumer.StickyAssignor;

/**
 * @program: finance-service
 * @description: 分配策略枚举
 * @author: ZengShiLin
 * @create: 3/16/2020 11:00 AM
 **/
public enum PartitionAssignorEnum {

    /**
     * 分配策略枚举
     */
    Range("范围平均分配策略", RangeAssignor.class),
    RoundRobin("随机分配策略", RoundRobinAssignor.class),
    Sticky("粘性分配策略", StickyAssignor.class);

    /**
     * 描述
     */
    @Getter
    private String describe;

    /**
     * 分配策略
     */
    @Getter
    private Class assignor;

    PartitionAssignorEnum(String describe, Class assignor) {
        this.describe = describe;
        this.assignor = assignor;
    }

}
