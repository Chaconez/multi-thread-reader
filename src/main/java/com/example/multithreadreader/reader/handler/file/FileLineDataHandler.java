package com.example.multithreadreader.reader.handler.file;


import com.example.multithreadreader.reader.parser.Parser;
import com.ke.calwage.schedule.previous.reader.handler.DataProcessHandler;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 文件处理器接口,处理文本类型文件
 *
 * @author zhangwenjie037
 */
public class FileLineDataHandler<T> implements DataProcessHandler<T> {


    private final String encode;

    Parser<T> parser;

    List<T> synchronizedList = Collections.synchronizedList((new ArrayList<>()));

    public Parser<T> getParser() {
        return parser;
    }

    public FileLineDataHandler(Parser<T> parser, String encode) {
        this.parser = parser;
        this.encode = encode;
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<T> getParseResult() {
        return synchronizedList;
    }

    @Override
    public void process(byte[] data) {
        try {
            String line = new String(data, encode);
            T object = parser.parse(line);
            synchronizedList.add(object);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void clear(){
        synchronizedList.clear();
    }
}

