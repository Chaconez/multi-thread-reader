package com.example.multithreadreader.reader.parser.impl;

import com.example.multithreadreader.reader.parser.Parser;
import com.google.common.base.Splitter;

import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class FileParser implements Parser<String> {
    @Override
    public String parse(String line) {
        List<String> elements = Splitter.on(" ").splitToList(line);
        return elements.get(0);
    }

    @Override
    public Parser<String> quiet() {
        return null;
    }

    @Override
    public Parser<String> noise() {
        return null;
    }
}
