package com.my.kafka.client.monitor;

import com.my.kafka.client.consumer.executor.MyConsumerExecutor;
import lombok.extern.slf4j.Slf4j;

import java.util.Objects;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @program: finance-service
 * @description: 线程监控
 * @author: ZengShiLin
 * @create: 12/24/2019 4:50 PM
 **/
@Slf4j
public class ThreadMonitor implements Monitor<Void> {

    @Override
    public Void execute() {
        ThreadPoolExecutor consumerPoolExecutor = MyConsumerExecutor.getConsumerPoolExecutor();
        if (Objects.nonNull(consumerPoolExecutor) && !consumerPoolExecutor.isShutdown()) {
            MyConsumerExecutor.getRunnableMaps().forEach((topic, runnable) -> {
                if (!runnable.isThreadActive() || !runnable.isRun()) {
                    log.error("topic:{},消费者线程已经停止", topic);
                }
            });
        }
        return null;
    }

}
