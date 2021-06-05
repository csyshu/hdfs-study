package com.csy.utils;

/**
 * 文件大小
 *
 * @author csy
 * @date 2021-03-25
 */
public class FileUtils {
    private final static int GB_LENGTH = 1073741824;
    private final static int MB_LENGTH = 1048576;
    private final static int KB_LENGTH = 1024;

    /**
     * 格式化资源大小
     *
     * @param length 真实资源大小
     * @return 文件大小
     */
    public static String formatFileSize(long length) {
        String result;
        int subStringIndex;
        if (length >= GB_LENGTH) {
            subStringIndex = String.valueOf((float) length / GB_LENGTH).indexOf(
                    ".");
            result = ((float) length / GB_LENGTH + "000").substring(0,
                    subStringIndex + 3) + "GB";
        } else if (length >= MB_LENGTH) {
            subStringIndex = String.valueOf((float) length / MB_LENGTH).indexOf(".");
            result = ((float) length / MB_LENGTH + "000").substring(0,
                    subStringIndex + 3) + "MB";
        } else if (length >= KB_LENGTH) {
            subStringIndex = String.valueOf((float) length / KB_LENGTH).indexOf(".");
            result = ((float) length / KB_LENGTH + "000").substring(0,
                    subStringIndex + 3) + "KB";
        } else {
            result = length + "B";
        }

        return result;
    }
}
