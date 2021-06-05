package com.csy.utils;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.permission.FsAction;

/**
 * 根据权限编码解析资源权限
 *
 * @author csy
 */
public class FsActionUtils {
    /**
     * 无权限
     */
    private static final String NP_NUM = "0";
    /**
     * 只读
     */
    private static final String OR_NUM = "1";
    /**
     * 只写
     */
    private static final String OW_NUM = "2";
    /**
     * 读写
     */
    private static final String RW_NUM = "3";
    /**
     * 读/执行
     */
    private static final String RX_NUM = "5";
    /**
     * 写/执行
     */
    private static final String WX_NUM = "6";
    /**
     * 读/写/执行
     */
    private static final String RWX_NUM = "7";

    /**
     * 读code
     */
    private static final String R_CODE = "r";
    /**
     * 写code
     */
    private static final String W_CODE = "w";
    /**
     * 执行code
     */
    private static final String X_CODE = "x";

    public static FsAction getFsAction(String actionCode) {
        FsAction fsAction = null;
        //无权限
        if (StringUtils.isBlank(actionCode) || isNoPermissionCode(actionCode)) {
            fsAction = FsAction.NONE;
        }

        //可读
        if (OR_NUM.equals(actionCode) || actionCode.toLowerCase().contains(R_CODE)) {
            fsAction = FsAction.READ;
        }

        //可读/写
        if (isRwCode(actionCode)) {
            fsAction = FsAction.READ_WRITE;
        }

        //可读/执行
        if (isRxCode(actionCode)) {
            fsAction = FsAction.READ_EXECUTE;
        }

        //只写
        if (OW_NUM.equals(actionCode) || (actionCode.toLowerCase().contains(W_CODE))) {
            fsAction = FsAction.WRITE;
        }

        //可写/执行
        if (isWxCode(actionCode)) {
            fsAction = FsAction.WRITE_EXECUTE;
        }

        //可读/写/执行
        if (isRwxCode(actionCode)) {
            fsAction = FsAction.ALL;
        }
        return fsAction;
    }

    private static boolean isNoPermissionCode(String actionCode) {
        return NP_NUM.equals(actionCode) ||
                (!actionCode.toLowerCase().contains(R_CODE)
                        && !actionCode.toLowerCase().contains(W_CODE)
                        && !actionCode.toLowerCase().contains(X_CODE));
    }

    private static boolean isRwxCode(String actionCode) {
        return RWX_NUM.equals(actionCode) ||
                (actionCode.toLowerCase().contains(R_CODE)
                        && actionCode.toLowerCase().contains(W_CODE)
                        && actionCode.toLowerCase().contains(X_CODE));
    }

    private static boolean isWxCode(String actionCode) {
        return WX_NUM.equals(actionCode)
                || (actionCode.toLowerCase().contains(W_CODE)
                && actionCode.toLowerCase().contains(X_CODE));
    }

    private static boolean isRxCode(String actionCode) {
        return RX_NUM.equals(actionCode) ||
                (actionCode.toLowerCase().contains(R_CODE)
                        && actionCode.toLowerCase().contains(X_CODE));
    }

    private static boolean isRwCode(String actionCode) {
        return RW_NUM.equals(actionCode)
                || (actionCode.toLowerCase().contains(R_CODE)
                && actionCode.toLowerCase().contains(W_CODE));
    }

}
