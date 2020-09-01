package com.my.kafka.client.message;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Date;

/**
 * @program: finance-service
 * @description:
 * @author: ZengShiLin
 * @create: 4/6/2020 10:59 AM
 **/
@Deprecated
@Data
@NoArgsConstructor
public class OldMessage<T> implements Serializable {

    /**
     * 消息Id-key
     */
    private String id;

    /**
     * 消息创建时间
     */
    private Date createTime;

    /**
     * 消息内容
     */
    private T content;

    public OldMessage(T content) {
        this.content = content;
    }
}
