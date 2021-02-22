package com.example.multithreadreader.reader.parser;

/**
 * Created by shiqining on 2017/8/1.
 */
public interface Parser<T> {

    T parse(String line);

    Parser<T> quiet();

    Parser<T> noise();
}
