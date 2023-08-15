package com.summer.kvstore.compaction;

import com.alibaba.fastjson.JSONObject;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.summer.kvstore.constants.KVConstants;
import com.summer.kvstore.model.Position;
import com.summer.kvstore.model.command.Command;
import com.summer.kvstore.model.command.RmCommand;
import com.summer.kvstore.model.sstable.SsTable;
import com.summer.kvstore.utils.BlockUtils;
import com.summer.kvstore.utils.ConvertUtil;
import com.summer.kvstore.utils.FileUtils;
import com.summer.kvstore.utils.LoggerUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.SerializationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * compaction执行器
 *
 * 触发机制：
 * level0根据sstable文件数
 * 其他level根据文件总大小
 */
public class Compactioner {
    private final Logger LOGGER = LoggerFactory.getLogger(Compactioner.class);

    /**
     * Level 0的sstable文件个数超过此阈值，触发compaction
     */
    private final static Integer L0_DUMP_MAX_FILE_NUM = 8;

    /**
     * 其他level的sstable文件总大小超阈值，触发compaction（单位：byte）
     *
     * (L_OTHER_DUMP_MAX_FILE_SIZE ^ level) MB
     */
    private final static Integer L_OTHER_DUMP_MAX_FILE_SIZE = 10;

    /**
     * 用来执行compaction的线程池
     */
    private ExecutorService compactionExecutor;

    /**
     * compaction执行结果
     */
    private Future<?> backgroundCompactionFuture;

    /**
     * 锁
     */
    private final ReentrantLock mutex = new ReentrantLock();

    public Compactioner() {
        ThreadFactory compactionThreadFactory = new ThreadFactoryBuilder()
                .setNameFormat("summer-kvstore-compaction-%s")
                .setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler()
                {
                    @Override
                    public void uncaughtException(Thread t, Throwable e)
                    {
                        e.printStackTrace();
                    }
                })
                .build();
        compactionExecutor = Executors.newSingleThreadExecutor(compactionThreadFactory);
    }

    /**
     * compaction操作
     *
     * @param levelMetaInfos 每一层的sstable信息
     * @param nextFileNumber 文件编号生成器
     */
    public void compaction(Map<Integer, List<SsTable>> levelMetaInfos,
                           AtomicLong nextFileNumber) {
        LoggerUtil.debug(LOGGER, "doBackgroundCompaction,try to acquire lock.....");
        mutex.lock();
        LoggerUtil.debug(LOGGER, "doBackgroundCompaction,try to acquire lock success.....");

        //同步执行
        try {
            Integer pickedLevel = pickCompactionLevel(levelMetaInfos);
            if (pickedLevel < 0) {
                return;
            }
            doBackgroundCompactionLevelI(levelMetaInfos, nextFileNumber, pickedLevel);
        } catch (Exception e) {
            LoggerUtil.error(LOGGER, e, "doBackgroundCompaction exception,", e);
        } finally {
            mutex.unlock();
        }

        /**
        //提交线程池异步执行
        this.backgroundCompactionFuture = compactionExecutor.submit(new Callable<Void>() {
            @Override
            public Void call() {
                try {
                    doBackgroundCompaction(levelMetaInfos, nextFileNumber);
                } catch (Exception e) {
                    System.out.println("doBackgroundCompaction,exception," + e);
                    e.printStackTrace();
                }
                return null;
            }
        });
         **/
    }

    /**
     * 找出当前需要进行compaction的level
     *
     * @param levelMetaInfos
     * @return
     */
    private Integer pickCompactionLevel(Map<Integer, List<SsTable>> levelMetaInfos) {
        for (int level = 0;level < KVConstants.SSTABLE_MAX_LEVEL;++level) {
            if (isTheLevelNeedCompaction(levelMetaInfos, level)) {
                return level;
            }
        }

        return -99;
    }

    /**
     * 判断当前level是否达到执行compaction的条件
     *
     * level 0:文件个数超过预定的阈值
     * level 1或其他:文件总大小超过预定阈值（level 1：10MB、level 2：100MB）
     *
     * <p>https://leveldb-handbook.readthedocs.io/zh/latest/compaction.html</p>
     *
     * @param levelMetaInfos
     * @param level
     * @return
     */
    private boolean isTheLevelNeedCompaction(Map<Integer, List<SsTable>> levelMetaInfos,
                                             Integer level) {
        if (CollectionUtils.isEmpty(levelMetaInfos.get(level))) {
            return false;
        }

        if (level.equals(0)) {
            return levelMetaInfos.get(0).size() > L0_DUMP_MAX_FILE_NUM;
        }

        List<SsTable> ssTables = levelMetaInfos.get(level);
        Long totalSize = 0L;
        for (SsTable ssTable : ssTables) {
            totalSize += ssTable.getTableMetaInfo().getDataLen();
        }

        return totalSize > Math.pow(L_OTHER_DUMP_MAX_FILE_SIZE, level) * 1000;
    }

    /**
     * 执行compaction操作-level i
     *
     * @param levelMetaInfos
     * @param nextFileNumber
     * @param level 执行compaction的level
     */
    private void doBackgroundCompactionLevelI(Map<Integer, List<SsTable>> levelMetaInfos,
                                              AtomicLong nextFileNumber,
                                              Integer level) throws IOException {
        //已达到最大层
        if (level.equals(KVConstants.SSTABLE_MAX_LEVEL)) {
            return;
        }

        LoggerUtil.debug(LOGGER, "doBackgroundCompaction begin,level=" + level + ",levelMetaInfos=" + levelMetaInfos);

        //找出和当前新产生的sstable存在重合key的sstable，和下一level的存在key重合的sstable进行合并，写入到下一level

        //挑选最近一次产生的sstable文件
        List<SsTable> levelSstables  = levelMetaInfos.get(level);
        Optional<SsTable> maxFileNumOptional = levelSstables.stream().max(Comparator.comparingLong(SsTable::getFileNumber));
        SsTable lastLevelSsTable = maxFileNumOptional.get();

        LoggerUtil.debug(LOGGER, "doBackgroundCompaction,lastLevelSsTable=" + lastLevelSsTable);

        //如果是level0，那么不同sstable之间key可能存在重合
        List<SsTable> overlappedLevelSSTableFileMetaInfos = new ArrayList<>();
        if (level.equals(0)) {
            overlappedLevelSSTableFileMetaInfos = findOverlapSstables(
                    lastLevelSsTable.getTableMetaInfo().getSmallestKey(),
                    lastLevelSsTable.getTableMetaInfo().getLargestKey(),
                    levelSstables);
            //level 0重合的sstable文件数可能特别少，需要保证至少有1/3个文件
            while (overlappedLevelSSTableFileMetaInfos.size() < (levelSstables.size() / 3 + 1)) {
                for (SsTable tmpSstable : levelSstables) {
                    if (!isExist(overlappedLevelSSTableFileMetaInfos, tmpSstable)) {
                        overlappedLevelSSTableFileMetaInfos.add(tmpSstable);
                    }
                }
            }
        } else {
            overlappedLevelSSTableFileMetaInfos.add(lastLevelSsTable);
        }

        LoggerUtil.debug(LOGGER, "doBackgroundCompaction,overlappedLevelSSTableFileMetaInfos=" + overlappedLevelSSTableFileMetaInfos);

        //计算一批sstable文件覆盖的key范围
        String smallestKey = null;
        String largestKey = null;
        for (SsTable ssTableFileMetaInfo : overlappedLevelSSTableFileMetaInfos) {
            if (smallestKey == null || smallestKey.compareTo(ssTableFileMetaInfo.getTableMetaInfo().getSmallestKey()) > 0) {
                smallestKey = ssTableFileMetaInfo.getTableMetaInfo().getSmallestKey();
            }
            if (largestKey == null || largestKey.compareTo(ssTableFileMetaInfo.getTableMetaInfo().getLargestKey()) < 0) {
                largestKey = ssTableFileMetaInfo.getTableMetaInfo().getLargestKey();
            }
        }

        LoggerUtil.debug(LOGGER, "doBackgroundCompaction,current level key range:[" + smallestKey + "," + largestKey + "]");

        //再获取level+1层存在重合key的所有sstable
        List<SsTable> overlappedNextLevelSSTableFileMetaInfos = findOverlapSstables(
                smallestKey, largestKey, levelMetaInfos.get(level + 1));

        LoggerUtil.debug(LOGGER, "doBackgroundCompaction,overlappedNextLevelSSTableFileMetaInfos=" + overlappedNextLevelSSTableFileMetaInfos);

        //合并为1个sstable文件，放置到level+1

        //l0所有文件排序，文件编号越小的表示越早的文件，相同的key，以最新的数据为准
        List<SsTable> overlappedSstables = new ArrayList<>();
        if (!CollectionUtils.isEmpty(overlappedLevelSSTableFileMetaInfos)) {
            overlappedSstables.addAll(overlappedLevelSSTableFileMetaInfos);
        }
        if (!CollectionUtils.isEmpty(overlappedNextLevelSSTableFileMetaInfos)) {
            overlappedSstables.addAll(overlappedNextLevelSSTableFileMetaInfos);
        }
        overlappedSstables.sort((sstabl1, sstable2) -> {
            if (sstabl1.getFileNumber() < sstable2.getFileNumber()) {
                return -1;
            } else if (sstabl1.getFileNumber() == sstable2.getFileNumber()) {
                return 0;
            } else {
                return 1;
            }
        });

        //合并后的结果
        ConcurrentSkipListMap<String, Command> mergedData = new ConcurrentSkipListMap<>();
        mergeSstables(overlappedSstables, mergedData);

        LoggerUtil.debug(LOGGER, "doBackgroundCompaction...开始生成的sstable文件,overlappedLevelSSTableFileMetaInfos"
                + overlappedLevelSSTableFileMetaInfos + ",overlappedNextLevelSSTableFileMetaInfos="
                + overlappedNextLevelSSTableFileMetaInfos + ",mergedData=" + mergedData);

        //生成新的sstable文件（注意超过阈值需要分类为多个文件）
        List<ConcurrentSkipListMap<String, Command>> mergedDataList = split(mergedData, level + 1);
        List<SsTable> nextLevelSstables = levelMetaInfos.get(level + 1);
        if (nextLevelSstables == null) {
            nextLevelSstables =  new ArrayList<>();
            levelMetaInfos.put(level + 1, nextLevelSstables);
        }
        for (ConcurrentSkipListMap<String, Command> tmpData : mergedDataList) {
            SsTable newSsTable = SsTable.createFromIndex(nextFileNumber.getAndIncrement(), 4,
                    tmpData, true, level + 1);
            LoggerUtil.info(LOGGER, "doBackgroundCompaction...生成新sstable文件:" + newSsTable.getFilePath());
            nextLevelSstables.add(newSsTable);
        }

        //删除不需要的sstable
        Iterator levelSstableIterator = levelSstables.iterator();
        while (levelSstableIterator.hasNext()) {
            SsTable tempSsTable = (SsTable) levelSstableIterator.next();
            if (containTheSstable(overlappedLevelSSTableFileMetaInfos, tempSsTable.getFileNumber())) {
                levelSstableIterator.remove();
                tempSsTable.close();
                File tmpSstableFile = new File(FileUtils.buildSstableFilePath(tempSsTable.getFileNumber(), level));
                LoggerUtil.info(LOGGER, "doBackgroundCompaction...删除sstable文件:" + tmpSstableFile.getName());
                tmpSstableFile.delete();
            }
        }
        Iterator nextLevelSstableIterator = nextLevelSstables.iterator();
        while (nextLevelSstableIterator.hasNext()) {
            SsTable tempSsTable = (SsTable) nextLevelSstableIterator.next();
            if (containTheSstable(overlappedNextLevelSSTableFileMetaInfos, tempSsTable.getFileNumber())) {
                nextLevelSstableIterator.remove();
                tempSsTable.close();
                File tmpSstableFile = new File(FileUtils.buildSstableFilePath(tempSsTable.getFileNumber(), level + 1));
                LoggerUtil.info(LOGGER, "doBackgroundCompaction...删除sstable文件:" + tmpSstableFile.getName());
                tmpSstableFile.delete();
            }
        }

        LoggerUtil.debug(LOGGER, "doBackgroundCompaction...完成level:" + level + "的compaction,levelMetaInfos=" + levelMetaInfos);
    }

    /**
     * 判断一个sstable列表里是否包含对应编号的sstable文件
     * @param ssTables
     * @param fileNumber
     * @return
     */
    private boolean containTheSstable(List<SsTable> ssTables, Long fileNumber) {
        for (SsTable ssTable : ssTables) {
            if (fileNumber.equals(ssTable.getFileNumber())) {
                return true;
            }
        }

        return false;
    }

    /**
     * 合并sstable内容
     *
     * @param ssTableList
     * @param mergedData
     */
    private void mergeSstables(List<SsTable> ssTableList, ConcurrentSkipListMap<String, Command> mergedData) {
        ssTableList.forEach(ssTable -> {
            Map<String, JSONObject> jsonObjectMap = readSstableContent(ssTable);
            for (Map.Entry<String, JSONObject> entry : jsonObjectMap.entrySet()) {
                Command command = ConvertUtil.jsonToCommand(entry.getValue());
                //删除数据处理
                if (command instanceof RmCommand) {
                    mergedData.remove(command.getKey());
                } else {
                    mergedData.put(command.getKey(), command);
                }
            }
        });
    }

    /**
     * 读取sstable内容到内存
     * @param ssTable
     * @return
     * @throws IOException
     */
    private Map<String, JSONObject> readSstableContent(SsTable ssTable) {
        Map<String, JSONObject> jsonObjectMap = new HashMap<>();

        try {
            TreeMap<String, Position> sparseIndex = ssTable.getSparseIndex();
            for (Position position : sparseIndex.values()) {
                JSONObject jsonObject = BlockUtils.readJsonObject(position, true,
                        ssTable.getTableFile());
                //遍历每个key
                for (Map.Entry entry : jsonObject.entrySet()) {
                    String key = (String)entry.getKey();
                    jsonObjectMap.put(key, jsonObject.getJSONObject(key));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return jsonObjectMap;
    }

    /**
     * 查找存在重合key的sstable
     *
     * @param smallestKey 起始key
     * @param largestKey 终止key
     * @param ssTables 某层的sstale信息
     * @return
     */
    private List<SsTable> findOverlapSstables(String smallestKey, String largestKey,
                                              List<SsTable> ssTables) {
        List<SsTable> ssTableFileMetaInfos = new ArrayList<>();
        if (ssTables == null) {
            return ssTableFileMetaInfos;
        }

        for (SsTable ssTable : ssTables) {
            if (!(ssTable.getTableMetaInfo().getLargestKey().compareTo(smallestKey) < 0
                    || ssTable.getTableMetaInfo().getSmallestKey().compareTo(largestKey) > 0)) {
                ssTableFileMetaInfos.add(ssTable);
            }
        }

        return ssTableFileMetaInfos;
    }

    /**
     * 将数据拆分为多个适合当前level大小的部分
     *
     * @param mergedData
     * @param level
     * @return
     */
    private List<ConcurrentSkipListMap<String, Command>> split(
            ConcurrentSkipListMap<String, Command> mergedData, Integer level) {
        List<ConcurrentSkipListMap<String, Command>> resultDataMaps = new ArrayList<>();
        ConcurrentSkipListMap<String, Command> map = new ConcurrentSkipListMap<>();
        for (Command command : mergedData.values()) {
            if (SerializationUtils.serialize(map).length > KVConstants.MAX_SSTABLE_SIZE) {
                resultDataMaps.add(map);
                map = new ConcurrentSkipListMap<>();
            }
            map.put(command.getKey(), command);
        }

        if (MapUtils.isNotEmpty(map)) {
            resultDataMaps.add(map);
        }

        return resultDataMaps;
    }

    private boolean isExist(List<SsTable> ssTables, SsTable ssTable) {
        if (CollectionUtils.isEmpty(ssTables)) {
            return false;
        }

        for (SsTable tmpSstable : ssTables) {
            if (tmpSstable.getFileNumber().equals(ssTable.getFileNumber())) {
                return true;
            }
        }

        return false;
    }
}
