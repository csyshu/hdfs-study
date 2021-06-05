package com.csy.utils;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IOUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.multipart.MultipartFile;

import javax.annotation.PostConstruct;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author shuyun.cheng
 */
@Slf4j
@Component
public class HdFsUtil2 {

    @Value("${hdfs.path}")
    private String path;
    @Value("${hdfs.username}")
    private String username;

    private static String hdfsPath;
    private static String hdfsName;
    private static final int BUFFER_SIZE = 1024 * 1024 * 64;

    @PostConstruct
    public void init() {
        hdfsPath = this.path;
        hdfsName = this.username;
    }

    /**
     * 获取HDFS配置信息
     *
     * @return Configuration
     */
    private static Configuration getConfiguration() {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", hdfsPath);
        return configuration;
    }

    /**
     * 获取HDFS文件系统对象
     *
     * @return FileSystem
     * @throws Exception 获取文件系统异常
     */
    public static FileSystem getFileSystem() throws Exception {
        // 客户端去操作hdfs时是有一个用户身份的，默认情况下hdfs客户端api会从jvm中获取一个参数作为自己的用户身份
        // DHADOOP_USER_NAME=hadoop
        // 也可以在构造客户端fs对象时，通过参数传递进去
        return FileSystem.get(new URI(hdfsPath), getConfiguration(), hdfsName);
    }

    /**
     * 在HDFS创建文件夹
     *
     * @param path 文件路径
     * @return boolean
     * @throws Exception 创建文件异常
     */
    public static boolean mkdir(String path) throws Exception {
        if (StringUtils.isEmpty(path)) {
            return false;
        }
        if (existFile(path)) {
            return true;
        }
        FileSystem fs = getFileSystem();
        // 目标路径
        Path srcPath = new Path(path);
        boolean isOk = fs.mkdirs(srcPath);
        fs.close();
        return isOk;
    }

    /**
     * 判断HDFS文件是否存在
     *
     * @param path 文件路径
     * @return boolean
     * @throws Exception 异常
     */
    public static boolean existFile(String path) throws Exception {
        if (StringUtils.isEmpty(path)) {
            return false;
        }
        FileSystem fs = getFileSystem();
        Path srcPath = new Path(path);
        return fs.exists(srcPath);
    }

    /**
     * 读取HDFS目录信息
     *
     * @param path 文件路径
     * @return List<Map < String, Object>>
     * @throws Exception 异常
     */
    public static List<Map<String, Object>> readPathInfo(String path) throws Exception {
        if (StringUtils.isEmpty(path)) {
            return null;
        }
        if (!existFile(path)) {
            return null;
        }
        FileSystem fs = getFileSystem();
        // 目标路径
        Path newPath = new Path(path);
        FileStatus[] statusList = fs.listStatus(newPath);
        List<Map<String, Object>> list = Lists.newArrayList();
        if (null != statusList && statusList.length > 0) {
            for (FileStatus fileStatus : statusList) {
                Map<String, Object> map = new HashMap<>();
                map.put("filePath", fileStatus.getPath());
                map.put("fileStatus", fileStatus.toString());
                list.add(map);
            }
            return list;
        } else {
            return Lists.newArrayList();
        }
    }

    /**
     * HDFS创建文件
     *
     * @param path 文件路径
     * @param file 文件
     * @throws Exception 异常
     */
    public static void createFile(String path, MultipartFile file) throws Exception {
        if (StringUtils.isEmpty(path)) {
            return;
        } else {
            file.getBytes();
        }
        String fileName = file.getOriginalFilename();
        FileSystem fs = getFileSystem();
        // 上传时默认当前目录，后面自动拼接文件的目录
        Path newPath = new Path(path + "/" + fileName);
        // 打开一个输出流
        FSDataOutputStream outputStream = fs.create(newPath);
        outputStream.write(file.getBytes());
        outputStream.close();
        fs.close();
    }

    /**
     * 读取HDFS文件内容
     *
     * @param path 文件路径
     * @return 文件内容
     * @throws Exception 异常
     */
    public static String readFile(String path) throws Exception {
        if (StringUtils.isEmpty(path)) {
            return null;
        }
        if (!existFile(path)) {
            return null;
        }
        FileSystem fs = getFileSystem();
        // 目标路径
        Path srcPath = new Path(path);
        FSDataInputStream inputStream = null;
        try {
            inputStream = fs.open(srcPath);
            // 防止中文乱码
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
            String lineTxt = "";
            StringBuilder sb = new StringBuilder();
            while ((lineTxt = reader.readLine()) != null) {
                sb.append(lineTxt);
            }
            return sb.toString();
        } finally {
            if (inputStream != null) {
                inputStream.close();
            }
            fs.close();
        }
    }

    /**
     * 读取HDFS文件列表
     *
     * @param path 文件路径
     * @return List<Map < String, String>>
     * @throws Exception 异常
     */
    public static List<Map<String, String>> listFile(String path) throws Exception {
        if (StringUtils.isEmpty(path)) {
            return null;
        }
        if (!existFile(path)) {
            return null;
        }
        FileSystem fs = getFileSystem();
        // 目标路径
        Path srcPath = new Path(path);
        // 递归找到所有文件
        RemoteIterator<LocatedFileStatus> filesList = fs.listFiles(srcPath, true);
        List<Map<String, String>> returnList = new ArrayList<>();
        while (filesList.hasNext()) {
            LocatedFileStatus next = filesList.next();
            String fileName = next.getPath().getName();
            Path filePath = next.getPath();
            Map<String, String> map = new HashMap<>();
            map.put("fileName", fileName);
            map.put("filePath", filePath.toString());
            returnList.add(map);
        }
        fs.close();
        return returnList;
    }

    /**
     * HDFS重命名文件
     *
     * @param oldName 旧名称
     * @param newName 新名称
     * @return boolean
     * @throws Exception 异常
     */
    public static boolean renameFile(String oldName, String newName) throws Exception {
        if (StringUtils.isEmpty(oldName) || StringUtils.isEmpty(newName)) {
            return false;
        }
        FileSystem fs = getFileSystem();
        // 原文件目标路径
        Path oldPath = new Path(oldName);
        // 重命名目标路径
        Path newPath = new Path(newName);
        boolean isOk = fs.rename(oldPath, newPath);
        fs.close();
        return isOk;
    }

    /**
     * 删除HDFS文件
     *
     * @param path 路径
     * @return boolean
     * @throws Exception 异常
     */
    public static boolean deleteFile(String path) throws Exception {
        if (StringUtils.isEmpty(path)) {
            return false;
        }
        if (!existFile(path)) {
            return false;
        }
        FileSystem fs = getFileSystem();
        Path srcPath = new Path(path);
        boolean isOk = fs.deleteOnExit(srcPath);
        fs.close();
        return isOk;
    }

    /**
     * 本次文件上传到HDFS文件
     *
     * @param sourcePath 本地文件路径
     * @param uploadPath 目标路径
     * @throws Exception 异常
     */
    public static void uploadFile(String sourcePath, String uploadPath) throws Exception {
        if (StringUtils.isEmpty(sourcePath) || StringUtils.isEmpty(uploadPath)) {
            return;
        }
        FileSystem fs = getFileSystem();
        // 上传路径
        Path localPath = new Path(sourcePath);
        // 目标路径
        Path serverPath = new Path(uploadPath);
        // 调用文件系统的文件复制方法，第一个参数是否删除原文件true为删除，默认为false
        fs.copyFromLocalFile(false, localPath, serverPath);
        fs.close();
    }

    /**
     * 下载HDFS文件
     *
     * @param sourcePath   源文件路径
     * @param downloadPath 下载目标路径
     * @throws Exception 异常
     */
    public static void downloadFile(String sourcePath, String downloadPath) throws Exception {
        if (StringUtils.isEmpty(sourcePath) || StringUtils.isEmpty(downloadPath)) {
            return;
        }
        FileSystem fs = getFileSystem();
        // 源路径
        Path serverPath = new Path(sourcePath);
        // 目标路径
        Path localPath = new Path(downloadPath);
        // 调用文件系统的文件复制方法，第一个参数是否删除原文件true为删除，默认为false
        fs.copyToLocalFile(false, serverPath, localPath);
        fs.close();
    }

    /**
     * HDFS文件复制
     *
     * @param sourcePath 源文件路径
     * @param targetPath 目标文件路径
     * @throws Exception 异常
     */
    public static void copyFile(String sourcePath, String targetPath) throws Exception {
        if (StringUtils.isEmpty(sourcePath) || StringUtils.isEmpty(targetPath)) {
            return;
        }
        FileSystem fs = getFileSystem();
        // 原始文件路径
        Path oldPath = new Path(sourcePath);
        // 目标路径
        Path newPath = new Path(targetPath);

        FSDataInputStream inputStream = null;
        FSDataOutputStream outputStream = null;
        try {
            inputStream = fs.open(oldPath);
            outputStream = fs.create(newPath);

            IOUtils.copyBytes(inputStream, outputStream, BUFFER_SIZE, false);
        } finally {
            if (inputStream != null) {
                inputStream.close();
            }
            if (outputStream != null) {
                outputStream.close();
            }
            fs.close();
        }
    }

    /**
     * 打开HDFS上的文件并返回byte数组
     *
     * @param path 路径
     * @return byte[]
     * @throws Exception 异常
     */
    public static byte[] openFileToBytes(String path) throws Exception {
        if (StringUtils.isEmpty(path)) {
            return null;
        }
        if (!existFile(path)) {
            return null;
        }
        // 目标路径
        try (FileSystem fs = getFileSystem()) {
            Path srcPath = new Path(path);
            FSDataInputStream inputStream = fs.open(srcPath);
            return IOUtils.readFullyToByteArray(inputStream);
        }
    }

    /**
     * 打开HDFS上的文件并返回java对象
     *
     * @param path 路径
     * @return T
     * @throws Exception 异常
     */
    public static <T> T openFileToObject(String path, Class<T> clazz) throws Exception {
        if (StringUtils.isEmpty(path)) {
            return null;
        }
        if (!existFile(path)) {
            return null;
        }
        String jsonStr = readFile(path);
        return JSON.parseObject(jsonStr, clazz);
    }

    /**
     * 获取某个文件在HDFS的集群位置
     *
     * @param path 路径
     * @return BlockLocation[]
     * @throws Exception 异常
     */
    public static BlockLocation[] getFileBlockLocations(String path) throws Exception {
        if (StringUtils.isEmpty(path)) {
            return null;
        }
        if (!existFile(path)) {
            return null;
        }
        FileSystem fs = getFileSystem();
        // 目标路径
        Path srcPath = new Path(path);
        FileStatus fileStatus = fs.getFileStatus(srcPath);
        return fs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
    }

}