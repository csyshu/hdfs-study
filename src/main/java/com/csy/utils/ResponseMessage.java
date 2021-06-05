package com.csy.utils;

import lombok.Data;

/**
 * @author csy
 */
@Data
public class ResponseMessage<T> {
    /**
     * 状态码
     */
    private Integer code;
    /**
     * 提示信息
     */
    private String msg;
    /**
     * 错误详情
     */
    private String error;
    /**
     * 成功返回的数据
     */
    private T data;
}
