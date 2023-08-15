package com.summer.kvstore.constants;

/**
 * 相关常量定义
 */
public class KVConstants {

    /**
     * 工作空间目录
     */
    public final static String WORK_DIR = "/tmp/kvstore/";

    /**
     * 文件后缀-sstable文件
     */
    public static final String FILE_SUFFIX_SSTABLE = ".sst";

    /**
     * sstable最大层数
     */
    public static final Integer SSTABLE_MAX_LEVEL = 3;

    /**
     * 单个sstable文件大小阈值，单位：byte（即1MB）（测试用先设置为1KB）
     */
    public final static Integer MAX_SSTABLE_SIZE = 1024;
}
