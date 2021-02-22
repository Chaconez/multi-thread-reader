package com.ke.calwage.schedule.previous.reader.task;

import com.ke.calwage.schedule.previous.reader.handler.DataProcessHandler;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.CountDownLatch;

/**
 * 多线程按行读取文件实现
 *
 * @author zhangwenjie037
 */
@Slf4j
public class FileReaderTask implements Runnable {

    private final int bufSize = 1024;

    private DataProcessHandler dataProcessHandler;

    private CountDownLatch countDownLatch;

    private String filePath;

    private long fileStartIndex;

    private long startIndex;

    private long rSize;

    public FileReaderTask(String filePath, DataProcessHandler dataProcessHandler,
                          CountDownLatch countDownLatch, long fileStartIndex, long startIndex, long rSize) {
        this.filePath = filePath;
        this.dataProcessHandler = dataProcessHandler;
        this.countDownLatch = countDownLatch;
        this.fileStartIndex = fileStartIndex;
        this.startIndex = startIndex > 0 ? startIndex - 1 : startIndex;
        this.rSize = rSize;
    }

    @Override
    public void run() {
        readByLine();
        countDownLatch.countDown();
    }

    /**
     * 按行读取文件实现逻辑
     *
     * @return
     */
    public void readByLine() {
        FileChannel channel = null;
        try {
            try (RandomAccessFile accessFile = new RandomAccessFile(filePath, "r")) {
                channel = accessFile.getChannel();
                ByteBuffer rebuff = ByteBuffer.allocate(bufSize);
                channel.position(startIndex);
                long endIndex = startIndex + rSize;
                byte[] temp = new byte[0];
                int lineFeed = "\n".getBytes()[0];
                boolean isEnd = false;
                boolean isWholeLine = false;
                long lineCount = 0;
                long endBuffIndex = startIndex;
                while (channel.read(rebuff) != -1 && !isEnd) {
                    int position = rebuff.position();
                    byte[] reByte = new byte[position];
                    rebuff.flip();
                    rebuff.get(reByte);
                    int startPosition = 0;
                    for (int i = 0; i < reByte.length; i++) {
                        endBuffIndex++;
                        if (reByte[i] == lineFeed) {
                            if (channel.position() == startIndex) {
                                isWholeLine = true;
                            } else {
                                byte[] line = new byte[temp.length + i - startPosition + 1];
                                System.arraycopy(temp, 0, line, 0, temp.length);
                                System.arraycopy(reByte, startPosition, line, temp.length, i - startPosition + 1);
                                lineCount++;
                                temp = new byte[0];
                                if (startIndex == fileStartIndex || lineCount != 1 || isWholeLine) {
                                    dataProcessHandler.process(line);
                                }
                                if (endBuffIndex >= endIndex) {
                                    isEnd = true;
                                    break;
                                }
                            }
                            startPosition = i + 1;
                        }
                    }
                    if (!isEnd && startPosition < reByte.length) {
                        byte[] temp2 = new byte[temp.length + reByte.length - startPosition];
                        System.arraycopy(temp, 0, temp2, 0, temp.length);
                        System.arraycopy(reByte, startPosition, temp2, temp.length, reByte.length - startPosition);
                        temp = temp2;
                    }
                    rebuff.clear();
                }

                if (temp.length > 0) {
                    if (dataProcessHandler != null) {
                        dataProcessHandler.process(temp);
                    }
                }
                log.info(Thread.currentThread().getName() + " 执行完毕----加载行数:" + lineCount);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                channel.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}