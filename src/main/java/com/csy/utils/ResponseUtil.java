package com.csy.utils;


import com.csy.entity.ResponseEnums;

/**
 * 接口响应工具类
 *
 * @author csy
 * @date 2021-03-25
 */
public class ResponseUtil<T> {

    /**
     * 操作成功，返回没有分页的列表
     *
     * @param records 记录
     * @return RequestMessage<T>
     */
    public static <T> ResponseMessage<T> successResponse(T records) {
        ResponseMessage<T> message = new ResponseMessage<>();
        message.setCode(ResponseEnums.SUCCESS.getCode());
        message.setMsg(ResponseEnums.SUCCESS.getMsg());
        message.setData(records);
        return message;
    }

    /**
     * 操作成功，只返回结果，没有记录
     *
     * @return ResponseMessage<T>
     */
    public static <T> ResponseMessage<T> successResponse(String msg) {
        ResponseMessage<T> message = new ResponseMessage<>();
        message.setCode(ResponseEnums.SUCCESS.getCode());
        if (null == msg || "".equals(msg)) {
            message.setMsg(ResponseEnums.SUCCESS.getMsg());
        } else {
            message.setMsg(msg);
        }
        return message;
    }

    /**
     * 操作失败，返回错误描述
     *
     * @param msg 错误信息
     * @return ResponseMessage<T>
     */
    public static <T> ResponseMessage<T> failedResponse(String msg) {
        ResponseMessage<T> message = new ResponseMessage<>();
        message.setCode(ResponseEnums.ERROR.getCode());
        if (null == msg || "".equals(msg)) {
            message.setMsg(ResponseEnums.ERROR.getMsg());
        } else {
            message.setMsg(msg);
        }
        return message;
    }

    /**
     * 操作失败，返回错误描述
     *
     * @param code 错误code
     * @param msg  错误信息
     * @return ResponseMessage<T>
     */
    public static <T> ResponseMessage<T> failedResponse(Integer code, String msg) {
        ResponseMessage<T> message = new ResponseMessage<>();
        if (null == code) {
            message.setCode(ResponseEnums.ERROR.getCode());
        } else {
            message.setCode(code);
        }
        if (null == msg || "".equals(msg)) {
            message.setMsg(ResponseEnums.ERROR.getMsg());
        } else {
            message.setMsg(msg);
        }
        return message;
    }

    /**
     * 操作失败，返回错误描述
     *
     * @param msg   错误信息
     * @param error 错误详情
     * @return ResponseMessage<T>
     */
    public static <T> ResponseMessage<T> failedResponse(String msg, String error) {
        ResponseMessage<T> message = new ResponseMessage<>();
        message.setCode(ResponseEnums.ERROR.getCode());
        if (null == msg || "".equals(msg)) {
            message.setMsg(ResponseEnums.ERROR.getMsg());
        } else {
            message.setMsg(msg);
        }
        message.setError(error);
        return message;
    }
}
