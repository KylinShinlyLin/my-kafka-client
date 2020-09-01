package com.my.kafka.client.monitor;

/**
 * @program: finance-service
 * @description:
 * @author: ZengShiLin
 * @create: 12/25/2019 2:14 PM
 **/
public interface Monitor<T> {

    /**
     * 监控执行
     *
     * @return 返回类型
     */
    T execute();
}
