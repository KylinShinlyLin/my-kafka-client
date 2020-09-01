package test.kafka;

import com.my.kafka.client.message.Message;
import com.my.kafka.client.message.OldMessage;
import com.my.kafka.client.producer.MyKafkaProducer;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.UUID;
import java.util.concurrent.ExecutionException;

/**
 * @program: finance-service
 * @description:
 * @author: ZengShiLin
 * @create: 4/6/2020 12:13 PM
 **/
@Slf4j
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath:spring-service.xml"})
public class TestKafka {


    @Autowired
    private MyKafkaProducer myKafkaProducer;

    @Test
    public void sendMessage() throws ExecutionException, InterruptedException {
        for (int i = 0; i < 100; i++) {
            myKafkaProducer.sendAsync(new Message<>("test-finance-service", "测试数据:" + i), true).get();
            System.out.println(String.format("发送消息: %S", "测试数据:" + i));
        }
        for (int i = 0; i < 100; i++) {
            myKafkaProducer.sendAsync(new Message<>("test-finance-service2", "测试数据:" + i), true).get();
            System.out.println(String.format("发送消息: %S", "测试数据:" + i));
        }
        for (int i = 0; i < 100; i++) {
            myKafkaProducer.sendAsync(new Message<>("test-finance-service3", "测试数据:" + i), true).get();
            System.out.println(String.format("发送消息: %S", "测试数据:" + i));
        }
    }


    @Test
    public void sendSingleMessage() {
        myKafkaProducer.sendSync(new Message<>(UUID.randomUUID().toString(), "test-topic", "测试数据"));
        System.out.println("发送测试数据");
    }

    @Test
    public void sendOldMessage() throws ExecutionException, InterruptedException {
        for (int i = 0; i < 100; i++) {
            myKafkaProducer.sendAsync("test-finance-service-old", new OldMessage<>("测试数据")).get();
            System.out.println("发送测试数据");
        }
    }

    @Test
    public void testConsumer() {
        while (true) {

        }
    }


}
