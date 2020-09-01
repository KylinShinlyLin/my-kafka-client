package com.my.kafka.client.producer;

import com.alibaba.fastjson.JSON;
import com.my.kafka.client.common.ConfigUtil;
import com.my.kafka.client.config.KafkaProducerConfig;
import com.my.kafka.client.exception.KafkaException;
import com.my.kafka.client.exception.KafkaExceptionEnum;
import com.my.kafka.client.message.Message;
import com.my.kafka.client.message.OldMessage;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Future;

/**
 * @program: kafka-test
 * @description: kafka生产者（支持池化和，ThreadLocal） KafkaProducer 是线程安全，但是事务应该是ThreadLocal的
 * @author: ZengShiLin
 * @create: 2019-07-09 09:06
 **/
@Slf4j
public class MyKafkaProducer {


    private static volatile MyKafkaProducer instance;

    /**
     * 生产者配置
     */
    private static KafkaProducerConfig producerConfig;

    /**
     * 线程本地生产者  使用ThreadLocal 事务隔离
     */
    private static ThreadLocal<KafkaProducer<String, String>> PRODUCER_THREADLOCAL;

    /**
     * kafka生产者池
     */
    private static KafkaProducerPool KafkaProducerPool;

    /**
     * 是否已经初始化（volatile 增强可见性）
     */
    private volatile static boolean INITIALIZE = false;

    public MyKafkaProducer initProducer() {
        if (null == PRODUCER_THREADLOCAL || null == PRODUCER_THREADLOCAL.get()) {
            synchronized (MyKafkaProducer.class) {
                Properties properties = ConfigUtil.getProducerproperties(producerConfig);
                KafkaProducer<String, String> producer;
                try {
                    producer = new KafkaProducer<>(properties);
                } catch (Exception e) {
                    e.printStackTrace();
                    throw new KafkaException(KafkaExceptionEnum.PRODUCER_INITIALIZE_FAIL.getCode()
                            , String.format(KafkaExceptionEnum.PRODUCER_INITIALIZE_FAIL.getDescription(), e.getMessage()));
                }
                if (producerConfig.isEnableTransactional()) {
                    producer.initTransactions();
                }
                //创建ThreadLocal生产者
                PRODUCER_THREADLOCAL = ThreadLocal.withInitial(() -> producer);
                //创建生产者池
                KafkaProducerPool = new KafkaProducerPool(producerConfig);
                INITIALIZE = true;
                Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown));
                log.info("MyKafkaProducer init success");
            }
        }
        return instance;
    }

    /**
     * 构造函数初始化（测试使用要去掉）
     *
     * @param config 生成者配置
     */
    public static MyKafkaProducer setConfig(KafkaProducerConfig config) {
        synchronized (MyKafkaProducer.class) {
            producerConfig = config;
            return instance == null ? (instance = new MyKafkaProducer()) : instance;
        }
    }


    private MyKafkaProducer() {
    }


    /**
     * 服务关闭
     */
    private synchronized void shutdown() {
        INITIALIZE = false;
        if (null != PRODUCER_THREADLOCAL && null != PRODUCER_THREADLOCAL.get()) {
            PRODUCER_THREADLOCAL.get().close();
            PRODUCER_THREADLOCAL.remove();
        }
        KafkaProducerPool.shutDown();
    }

//    TODO 事物功能用的不多，先屏蔽
//    /**
//     * 开启事务（如果代码块里面开启了异步线程那么事务不会生效）
//     *
//     * @param execute 需要执行的代码
//     */
//    public void openTransaction(TransactionExecute execute) {
//        try {
//            PRODUCER_THREADLOCAL.get().beginTransaction();
//        } catch (Exception e) {
//            e.getMessage();
//            throw new KafkaException(KafkaExceptionEnum.PRODUCER_OPEN_TRANSACTION_FAILURE.getCode()
//                    , String.format(KafkaExceptionEnum.PRODUCER_OPEN_TRANSACTION_FAILURE.getDescription(), e.getMessage()));
//        }
//        try {
//            execute.doInTransaction();
//            //提交事务
//            PRODUCER_THREADLOCAL.get().commitTransaction();
//        } catch (Exception e) {
//            //回滚事务
//            PRODUCER_THREADLOCAL.get().abortTransaction();
//            log.error("transaction roll back:", e);
//            throw e;
//        }
//    }

    /**
     * 同步发送（等待同步响应）
     * 同步发送，事务将会失效
     *
     * @param message 需要发送的消息
     */
    public <T> void sendSync(Message<T> message) {
        //没有初始化不给发送
        this.sendSync(message, false);
    }


    /**
     * 同步发送（等待同步响应）
     * 同步发送，事务将会失效
     *
     * @param message 需要发送的消息
     * @param usePool 是否使用生产者池
     */
    public <T> void sendSync(Message<T> message, boolean usePool) {
        //没有初始化不给发送
        if (!INITIALIZE) {
            throw new KafkaException(KafkaExceptionEnum.PRODUCER_THREADLOCAL_INITIALIZE_FAILURE.getCode()
                    , KafkaExceptionEnum.PRODUCER_THREADLOCAL_INITIALIZE_FAILURE.getDescription());
        }
        try {
            ProducerRecord<String, String> record = this.getProducerRecord(message);
            if (usePool) {
                KafkaProducerPool.execSync(record);
            } else {
                PRODUCER_THREADLOCAL.get().send(record).get();
                log.info("message send success topic:{},key:{}", message.getTopic(), message.getKey());
            }
        } catch (Exception e) {
            log.error("message send failed:", e);
            e.printStackTrace();
            throw new KafkaException(KafkaExceptionEnum.PRODUCER_SEND_FAILURE.getCode(),
                    KafkaExceptionEnum.PRODUCER_SEND_FAILURE.getDescription() + e.getMessage());
        }
    }


    /**
     * 异步发送（无回调，发了就不管了）推荐使用 TODO 用来兼容旧版本发送
     *
     * @param message 消息内容
     * @param <T>     消息体泛型
     * @return Future<RecordMetadata>
     */
    @Deprecated
    public <T> Future<RecordMetadata> sendAsync(String topic, OldMessage<T> message) {
        //没有初始化不给发送
        if (!INITIALIZE) {
            throw new KafkaException(KafkaExceptionEnum.PRODUCER_THREADLOCAL_INITIALIZE_FAILURE.getCode()
                    , KafkaExceptionEnum.PRODUCER_THREADLOCAL_INITIALIZE_FAILURE.getDescription());
        }
        if (StringUtils.isBlank(topic)) {
            throw new KafkaException(KafkaExceptionEnum.PRODUCER_NOT_ASSIGN_TOPIC.getCode()
                    , KafkaExceptionEnum.PRODUCER_NOT_ASSIGN_TOPIC.getDescription());
        }
        try {
            ProducerRecord<String, String> record = this.getProducerRecord(topic, message);
            log.info("prepare send message,topic:{},key:{}", topic, message.getId());
            return PRODUCER_THREADLOCAL.get().send(record, (metadata, exception) -> {
                if (exception != null) {
                    log.error("message send failed:", exception);
                    throw new KafkaException(KafkaExceptionEnum.PRODUCER_SEND_FAILURE.getCode(),
                            KafkaExceptionEnum.PRODUCER_SEND_FAILURE.getDescription() + exception.getMessage());
                }
                log.info("message send success ,topic:{},key:{}", topic, message.getId());
            });
        } catch (Exception e) {
            log.error("message send failed:", e);
            e.printStackTrace();
            throw new KafkaException(KafkaExceptionEnum.PRODUCER_SEND_FAILURE.getCode(),
                    KafkaExceptionEnum.PRODUCER_SEND_FAILURE.getDescription() + e.getMessage());
        }
    }


    /**
     * 异步发送（无回调，发了就不管了）推荐使用
     *
     * @param message 消息内容
     * @param <T>     消息体泛型
     * @return Future<RecordMetadata>
     */
    public <T> Future<RecordMetadata> sendAsync(Message<T> message) {
//        //没有初始化不给发送
//        if (!INITIALIZE) {
//            throw new KafkaException(KafkaExceptionEnum.PRODUCER_THREADLOCAL_INITIALIZE_FAILURE.getCode()
//                    , KafkaExceptionEnum.PRODUCER_THREADLOCAL_INITIALIZE_FAILURE.getDescription());
//        }
//        try {
//            ProducerRecord<String, String> record = this.getProducerRecord(message);
//            log.info("prepare send message,topic:{},key:{}", message.getTopic(), message.getKey());
//            return PRODUCER_THREADLOCAL.get().send(record, (metadata, exception) -> {
//                if (exception != null) {
//                    log.error("message send failed:", exception);
//                    throw new KafkaException(KafkaExceptionEnum.PRODUCER_SEND_FAILURE.getCode(),
//                            KafkaExceptionEnum.PRODUCER_SEND_FAILURE.getDescription() + exception.getMessage());
//                }
//                log.info("message send success ,topic:{},key:{}", message.getTopic(), message.getKey());
//            });
//        } catch (Exception e) {
//            log.error("message send failed:", e);
//            e.printStackTrace();
//            throw new KafkaException(KafkaExceptionEnum.PRODUCER_SEND_FAILURE.getCode(),
//                    KafkaExceptionEnum.PRODUCER_SEND_FAILURE.getDescription() + e.getMessage());
//        }
        return this.sendAsync(message, false);
    }

    /**
     * 异步发送（无回调，发了就不管了）推荐使用
     *
     * @param message 消息内容
     * @param <T>     消息体泛型
     * @param usePool 是否使用生产者池
     * @return Future<RecordMetadata>
     */
    public <T> Future<RecordMetadata> sendAsync(Message<T> message, boolean usePool) {
        //没有初始化不给发送
        if (!INITIALIZE) {
            throw new KafkaException(KafkaExceptionEnum.PRODUCER_THREADLOCAL_INITIALIZE_FAILURE.getCode()
                    , KafkaExceptionEnum.PRODUCER_THREADLOCAL_INITIALIZE_FAILURE.getDescription());
        }
        try {
            ProducerRecord<String, String> record = this.getProducerRecord(message);
            if (usePool) {
                return KafkaProducerPool.execAsync(record);
            } else {
                log.info("prepare send message,topic:{},key:{}", message.getTopic(), message.getKey());
                return PRODUCER_THREADLOCAL.get().send(record, (metadata, exception) -> {
                    if (exception != null) {
                        log.error("message send failed:", exception);
                        throw new KafkaException(KafkaExceptionEnum.PRODUCER_SEND_FAILURE.getCode(),
                                KafkaExceptionEnum.PRODUCER_SEND_FAILURE.getDescription() + exception.getMessage());
                    }
                    log.info("message send success ,topic:{},key:{}", message.getTopic(), message.getKey());
                });
            }
        } catch (Exception e) {
            log.error("message send failed:", e);
            e.printStackTrace();
            throw new KafkaException(KafkaExceptionEnum.PRODUCER_SEND_FAILURE.getCode(),
                    KafkaExceptionEnum.PRODUCER_SEND_FAILURE.getDescription() + e.getMessage());
        }
    }

    /**
     * 异步自定义回调发送
     *
     * @param message  消息内容
     * @param callback 回调内容
     * @return Future<RecordMetadata>
     */
    public <T> Future<RecordMetadata> sendAsync(Message<T> message, Callback callback) {
        return this.sendAsync(message, callback);
    }

    /**
     * 异步自定义回调发送
     *
     * @param message  消息内容
     * @param callback 回调内容
     * @param usePool  是否使用生产者池
     * @return Future<RecordMetadata>
     */
    public <T> Future<RecordMetadata> sendAsync(Message<T> message, Callback callback, boolean usePool) {
        //没有初始化不给发送
        if (!INITIALIZE) {
            throw new KafkaException(KafkaExceptionEnum.PRODUCER_THREADLOCAL_INITIALIZE_FAILURE.getCode()
                    , KafkaExceptionEnum.PRODUCER_THREADLOCAL_INITIALIZE_FAILURE.getDescription());
        }
        try {
            ProducerRecord<String, String> record = this.getProducerRecord(message);
            if (usePool) {
                return KafkaProducerPool.execAsync(record, callback);
            } else {
                log.info("prepare send message,topic:{},key:{}", message.getTopic(), message.getKey());
                return PRODUCER_THREADLOCAL.get().send(record, callback);
            }
        } catch (Exception e) {
            log.error("message send failed:", e);
            throw new KafkaException(KafkaExceptionEnum.PRODUCER_SEND_FAILURE.getCode(),
                    KafkaExceptionEnum.PRODUCER_SEND_FAILURE.getDescription() + e.getMessage());
        }
    }

    /**
     * 获取生产者 record
     *
     * @param message 需要发送的消息
     * @param <T>     泛型
     * @return 返回需要发送的记录
     */
    private <T> ProducerRecord<String, String> getProducerRecord(Message<T> message) {
        //允许生成者发生消息到指定partition
        return null != message.getPartition() && message.getPartition() != Message.NULL ?
                new ProducerRecord<>(
                        message.getTopic(),
                        message.getPartition(),
                        System.currentTimeMillis(),
                        message.getKey(),
                        JSON.toJSONString(message.getValue())) :
                new ProducerRecord<>(
                        message.getTopic(),
                        Optional.ofNullable(message.getKey()).filter(StringUtils::isNoneBlank).orElse(UUID.randomUUID().toString()),
                        JSON.toJSONString(message.getValue()));
    }

    /**
     * 获取生产者 record
     *
     * @param message 需要发送的消息
     * @param <T>     泛型
     * @return 返回需要发送的记录
     */
    @Deprecated
    private <T> ProducerRecord<String, String> getProducerRecord(String topic, OldMessage<T> message) {
        //允许生成者发生消息到指定partition
        return new ProducerRecord<>(
                topic,
                message.getId(),
                JSON.toJSONString(message));
    }

    /**
     * 获取kafka实例，通过静态方法来打破spring循环依赖
     *
     * @return MyKafkaProducer 实例
     */
    public static MyKafkaProducer getInstance() {
        if (!INITIALIZE) {
            throw new KafkaException(KafkaExceptionEnum.PRODUCER_DON_NOT_INIT.getCode(),
                    KafkaExceptionEnum.PRODUCER_DON_NOT_INIT.getDescription());
        }
        return instance;
    }

}
