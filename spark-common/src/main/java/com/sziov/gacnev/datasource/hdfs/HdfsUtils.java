package com.sziov.gacnev.datasource.hdfs;

import com.sziov.gacnev.utils.WarehouseException;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Objects;

/**
 * HDFS工具类
 * 提供HDFS文件操作功能
 *
 * @author maikou
 * @since 2026-05-17
 */
@Slf4j
public final class HdfsUtils {

    private HdfsUtils() {
        throw new UnsupportedOperationException("工具类不允许实例化");
    }

    /**
     * 获取FileSystem
     *
     * @return FileSystem对象
     */
    public static FileSystem getFileSystem() {
        try {
            Configuration conf = new Configuration();
            return FileSystem.get(conf);
        } catch (IOException e) {
            log.error("Failed to get FileSystem", e);
            throw new WarehouseException("Failed to get FileSystem", e);
        }
    }

    /**
     * 获取FileSystem（指定配置）
     *
     * @param conf Configuration对象
     * @return FileSystem对象
     */
    public static FileSystem getFileSystem(Configuration conf) {
        try {
            return FileSystem.get(conf);
        } catch (IOException e) {
            log.error("Failed to get FileSystem", e);
            throw new WarehouseException("Failed to get FileSystem", e);
        }
    }

    /**
     * 关闭FileSystem
     *
     * @param fs FileSystem对象
     */
    public static void closeFileSystem(FileSystem fs) {
        if (Objects.nonNull(fs)) {
            try {
                fs.close();
                log.debug("FileSystem closed successfully");
            } catch (IOException e) {
                log.error("Failed to close FileSystem", e);
            }
        }
    }

    /**
     * 判断路径是否存在
     *
     * @param fs   FileSystem对象
     * @param path HDFS路径
     * @return 是否存在
     */
    public static boolean exists(FileSystem fs, String path) {
        try {
            return fs.exists(new Path(path));
        } catch (IOException e) {
            log.error("Failed to check path existence: {}", path, e);
            return false;
        }
    }

    /**
     * 创建目录
     *
     * @param fs   FileSystem对象
     * @param path 目录路径
     * @return 是否成功
     */
    public static boolean mkdirs(FileSystem fs, String path) {
        try {
            boolean result = fs.mkdirs(new Path(path));
            if (result) {
                log.info("Directory created: {}", path);
            }
            return result;
        } catch (IOException e) {
            log.error("Failed to create directory: {}", path, e);
            return false;
        }
    }

    /**
     * 删除路径
     *
     * @param fs      FileSystem对象
     * @param path    路径
     * @param recursive 是否递归删除
     * @return 是否成功
     */
    public static boolean delete(FileSystem fs, String path, boolean recursive) {
        try {
            boolean result = fs.delete(new Path(path), recursive);
            if (result) {
                log.info("Path deleted: {}, recursive: {}", path, recursive);
            }
            return result;
        } catch (IOException e) {
            log.error("Failed to delete path: {}", path, e);
            return false;
        }
    }

    /**
     * 重命名路径
     *
     * @param fs      FileSystem对象
     * @param oldPath 旧路径
     * @param newPath 新路径
     * @return 是否成功
     */
    public static boolean rename(FileSystem fs, String oldPath, String newPath) {
        try {
            boolean result = fs.rename(new Path(oldPath), new Path(newPath));
            if (result) {
                log.info("Path renamed from {} to {}", oldPath, newPath);
            }
            return result;
        } catch (IOException e) {
            log.error("Failed to rename path from {} to {}", oldPath, newPath, e);
            return false;
        }
    }

    /**
     * 上传本地文件到HDFS
     *
     * @param fs        FileSystem对象
     * @param localPath 本地文件路径
     * @param hdfsPath  HDFS路径
     * @return 是否成功
     */
    public static boolean uploadFile(FileSystem fs, String localPath, String hdfsPath) {
        try {
            fs.copyFromLocalFile(new Path(localPath), new Path(hdfsPath));
            log.info("File uploaded from {} to {}", localPath, hdfsPath);
            return true;
        } catch (IOException e) {
            log.error("Failed to upload file from {} to {}", localPath, hdfsPath, e);
            return false;
        }
    }

    /**
     * 从HDFS下载文件到本地
     *
     * @param fs        FileSystem对象
     * @param hdfsPath  HDFS路径
     * @param localPath 本地文件路径
     * @return 是否成功
     */
    public static boolean downloadFile(FileSystem fs, String hdfsPath, String localPath) {
        try {
            fs.copyToLocalFile(new Path(hdfsPath), new Path(localPath));
            log.info("File downloaded from {} to {}", hdfsPath, localPath);
            return true;
        } catch (IOException e) {
            log.error("Failed to download file from {} to {}", hdfsPath, localPath, e);
            return false;
        }
    }
}