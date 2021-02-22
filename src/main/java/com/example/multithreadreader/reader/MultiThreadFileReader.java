package com.example.multithreadreader.reader;


import com.example.multithreadreader.reader.handler.file.FileLineDataHandler;
import com.example.multithreadreader.reader.parser.impl.FileParser;
import com.ke.calwage.schedule.previous.reader.handler.DataProcessHandler;
import com.ke.calwage.schedule.previous.reader.task.FileReaderTask;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.concurrent.*;

/**
 * 多线程按行读取文件工具类
 *
 * @author zhangwenjie037
 */
@Component
@Slf4j
public class MultiThreadFileReader {

    private int threadNum = 6;


    private String filePath;

    private DataProcessHandler dataProcessHandler;

    private CountDownLatch countDownLatch;

    private final ThreadFactory readerThreadFactory = new CustomizableThreadFactory("Reader-Thread-pool-");

    public DataProcessHandler getDataProcessHandler() {
        return dataProcessHandler;
    }

    /**
     * 注册数据处理接口
     *
     * @param file           文件
     * @param threadNum      线程数
     * @param dataHandler    数据处理句柄
     * @param countDownLatch 同步锁
     */
    public void registerHandler(File file, int threadNum, DataProcessHandler dataHandler, CountDownLatch countDownLatch) {
        this.threadNum = threadNum;
        this.filePath = file.getAbsolutePath();
        this.dataProcessHandler = dataHandler;
        this.countDownLatch = countDownLatch;
    }

    /**
     * 获取文件起始位置
     *
     * @param fileChannel 文件通道
     * @param skipHead    是否跳过首行
     * @return 跳过首行后的其实位置
     * @throws IOException
     */
    private long getFileStartIndex(FileChannel fileChannel, Boolean skipHead) throws IOException {
        long fileStartIndex = 0;
        if (skipHead) {
            int lineFeed = "\n".getBytes()[0];
            ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
            while (fileChannel.read(byteBuffer) != -1) {
                for (int i = 0; i < byteBuffer.capacity(); i++) {
                    fileStartIndex++;
                    if (byteBuffer.get(i) == lineFeed) {
                        return fileStartIndex;
                    }
                }
            }
        }
        return fileStartIndex;
    }

    /**
     * 启动多线程读取文件
     */
    public void startRead(Boolean skipHead) throws InterruptedException {
        ExecutorService threadPool = new ThreadPoolExecutor(threadNum, threadNum,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(), readerThreadFactory);
        FileChannel fileChannel = null;
        try {
            long start = System.currentTimeMillis();
            try (RandomAccessFile randomAccessFile = new RandomAccessFile(filePath, "r")) {
                fileChannel = randomAccessFile.getChannel();
                long fileStartIndex = getFileStartIndex(fileChannel, skipHead);
                long size = fileChannel.size();
                size = size - fileStartIndex;
                long subSize = size / threadNum;
                for (int i = 0; i < threadNum; i++) {
                    long startIndex = fileStartIndex + i * subSize;
                    //除不尽放最后一个线程
                    if (size % threadNum > 0 && i == threadNum - 1) {
                        subSize += size % threadNum;
                    }
                    if (!threadPool.isShutdown()) {
                        threadPool.execute(new FileReaderTask(filePath, dataProcessHandler, countDownLatch, fileStartIndex, startIndex, subSize));
                    }
                }
            }
            this.countDownLatch.await();
            threadPool.shutdown();
            long end = System.currentTimeMillis();
            log.info("多线程加载器: 线程数--[" + threadNum + "]-- 成功加载 --[" + this.dataProcessHandler.getParseResult().size() + "]--行数据,耗时--[" + (end - start) + "]--毫秒");
        } catch (IOException e1) {
            e1.printStackTrace();
        } finally {
            try {
                if (fileChannel != null) {
                    fileChannel.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    public static void main(String[] args) throws Exception {
        System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
        long start = System.currentTimeMillis();
        MultiThreadFileReader multiThreadFileReader = new MultiThreadFileReader();
        File file = new File("/Users/zhangwenjie/data0/www/privdata/file.csv");
        int num = 6;
        multiThreadFileReader.registerHandler(file, num, new FileLineDataHandler(new FileParser(), "UTF-8"), new CountDownLatch(num));
        multiThreadFileReader.startRead(true);
        List<Object> list = multiThreadFileReader.dataProcessHandler.getParseResult();

        System.out.println("大小:" + list.size());
        long end = System.currentTimeMillis();
        System.out.println("耗时:" + (end - start) + "毫秒");
        System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
    }

}