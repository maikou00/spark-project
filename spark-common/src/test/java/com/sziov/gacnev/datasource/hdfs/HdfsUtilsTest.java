package com.sziov.gacnev.datasource.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link HdfsUtils} 测试用例。
 *
 * @author maikou
 * @since 2026-06-09
 */
@DisplayName("HdfsUtils HDFS工具测试")
class HdfsUtilsTest {

    @Test
    @DisplayName("getFileSystem_无参_返回FileSystem")
    void getFileSystem_noArgs_returnsFileSystem() throws Exception {
        FileSystem fs = HdfsUtils.getFileSystem();
        assertThat(fs).isNotNull();
    }

    @Test
    @DisplayName("getFileSystem_指定Configuration_返回FileSystem")
    void getFileSystem_withConfiguration_returnsFileSystem() throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = HdfsUtils.getFileSystem(conf);
        assertThat(fs).isNotNull();
    }

    @Test
    @DisplayName("closeFileSystem_有效FileSystem_关闭不抛异常")
    void closeFileSystem_validFileSystem_closesWithoutException() throws Exception {
        FileSystem fs = HdfsUtils.getFileSystem();
        HdfsUtils.closeFileSystem(fs);
    }

    @Test
    @DisplayName("closeFileSystem_null_不抛异常")
    void closeFileSystem_nullFileSystem_noException() {
        HdfsUtils.closeFileSystem(null);
    }
}
