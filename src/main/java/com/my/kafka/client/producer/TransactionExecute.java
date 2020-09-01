package com.my.kafka.client.producer;

/**
 * @program: kafka-test
 * @description: 事务执行接口
 * @author: ZengShiLin
 * @create: 2019-07-09 12:20
 **/
public interface TransactionExecute {

    /**
     * 按照事务的方式执行代码
     */
    void doInTransaction();
}
