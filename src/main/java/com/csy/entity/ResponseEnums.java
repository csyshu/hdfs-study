package com.csy.entity;


import lombok.Getter;

/**
 * code码枚举统一管理
 *
 * @author shuyun.cheng
 * @date 2021-06-05 16:02
 */
@Getter
public enum ResponseEnums {
    /**
     * 常用结果枚举
     */
    SUCCESS(200, "SUCCESS"),
    ERROR(500, "ERROR");

    private final Integer code;
    private final String msg;

    ResponseEnums(Integer code, String msg) {
        this.code = code;
        this.msg = msg;
    }

}
