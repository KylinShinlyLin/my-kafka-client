package com.my.kafka.client.producer;

import com.my.kafka.client.common.ConfigUtil;
import com.my.kafka.client.config.KafkaProducerConfig;
import com.my.kafka.client.exception.KafkaException;
import com.my.kafka.client.exception.KafkaExceptionEnum;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.Future;

/**
 * @program: finance-service
 * @description: 为什么要创建生产者对象池呢？
 * 虽然kafkaProducer是线程安全的，但是通过单一的TCP链接发送消息吞吐量难免低下。
 * 在对顺序没有严格要求的情况下，提供了生产者池的方式，方便发送大量消息
 * @author: ZengShiLin
 * @create: 4/23/2020 2:27 PM
 **/
@Slf4j
public class KafkaProducerPool {

    /**
     * 生产者对象池(使用数组更加高效)
     */
    private static KafkaProducer<String, String>[] producerPool;

    /**
     * 生产者的数量
     */
    private static int NUM;

    public void shutDown() {
        //释放生产者
        Arrays.stream(producerPool).forEach(KafkaProducer::close);
    }


    /**
     * 创建对象池
     *
     * @param producerConfig kafka生成配置
     */
    public KafkaProducerPool(KafkaProducerConfig producerConfig) {
        synchronized (KafkaProducerPool.class) {
            producerPool = new KafkaProducer[producerConfig.getProducerNum()];
            Properties properties = ConfigUtil.getProducerproperties(producerConfig);
            for (int i = 0; i < producerConfig.getProducerNum(); i++) {
                //创建生产者
                producerPool[i] = new KafkaProducer<>(properties);
            }
            NUM = producerConfig.getProducerNum();
        }
    }

    /**
     * 异步执行
     *
     * @param record 需要发送的记录
     */
    public Future<RecordMetadata> execAsync(ProducerRecord<String, String> record) {
        KafkaProducer<String, String> producer;
        log.info("prepare send message,topic:{},key:{}", record.topic(), record.key());
        return producerPool[this.getIndex(record, NUM)].send(record, (metadata, exception) -> {
            if (exception != null) {
                log.error("message send failed:", exception);
                throw new KafkaException(KafkaExceptionEnum.PRODUCER_SEND_FAILURE.getCode(),
                        KafkaExceptionEnum.PRODUCER_SEND_FAILURE.getDescription() + exception.getMessage());
            }
            log.info("message send success ,topic:{},key:{}", record.topic(), record.key());
        });
    }


    /**
     * 同步执行
     *
     * @param record 需要发送的记录
     * @throws InterruptedException 中断异常
     */
    public void execSync(ProducerRecord<String, String> record) throws Exception {
        producerPool[this.getIndex(record, NUM)].send(record).get();
        log.info("message send success topic:{},key:{}", record.topic(), record.key());
    }

    /**
     * 异步执行，入参带回调
     *
     * @param record 需要发送的记录
     */
    public Future<RecordMetadata> execAsync(ProducerRecord<String, String> record, Callback callback) {
        KafkaProducer<String, String> producer = null;
        log.info("prepare send message,topic:{},key:{}", record.topic(), record.key());
        return producerPool[this.getIndex(record, NUM)].send(record, callback);
    }

    /**
     * 对应index
     *
     * @param record 消息
     * @param num    数量
     * @return index
     */
    private int getIndex(ProducerRecord<String, String> record, int num) {
        return (num - 1) & record.key().hashCode();
    }

}
