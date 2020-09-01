package test.kafka;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.Lists;
import com.my.kafka.client.annotation.MyConsumerConfig;
import com.my.kafka.client.consumer.executor.BatchMessageExecutor;
import com.my.kafka.client.message.Message;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * @program: finance-service
 * @description:
 * @author: ZengShiLin
 * @create: 4/6/2020 1:24 PM
 **/
@MyConsumerConfig(maxPollRecords = 1, printConsumerTime = true)
@Component
public class TestConsumer extends BatchMessageExecutor {
    @Override
    public void execute(List<Message<String>> messages) {
        for (Message<String> message : messages) {
            System.out.println(String.format("Topic:%s 收到消息: %s", message.getTopic(), JSON.toJSONString(message)));
        }
    }


    @Override
    public List<String> getTopics() {
        return Lists.newArrayList(
                "test-finance-service",
                "test-finance-service2",
                "test-finance-service3",
                "test-topic", "test-finance-service-old");
    }
}
