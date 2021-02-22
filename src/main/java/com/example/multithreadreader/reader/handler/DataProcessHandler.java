package com.ke.calwage.schedule.previous.reader.handler;

import java.util.List;

/**
 * 数据处理器接口
 *
 * @author zhangwenjie037
 */
public interface DataProcessHandler<T> {

    /**
     * 数据解析返回
     * @return 返回解析数据集合
     */
    <T> List<T> getParseResult();

    /**
     * 数据处理
     * @param data 代处理byte数组
     */
    void process(byte[] data);

    /**
     * 清理内置集合数据
     */
    void clear();
}