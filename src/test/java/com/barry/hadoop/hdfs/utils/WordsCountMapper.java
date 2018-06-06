package com.barry.hadoop.hdfs.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Auther: hants
 * @Date: 2018-06-06 14:07
 * @Description: words count impl
 */
public class WordsCountMapper implements Mapper {

    private static final Logger logger = LoggerFactory.getLogger(WordsCountMapper.class);

    @Override
    public void map(String line, Context context) {
        String[] words = line.split(" ");
        for (String word : words){
            Object value = context.get(word);
            if (value==null){
                context.write(word, 1);
            }else {
                Integer v = (Integer) value;
                context.write(word, v+1);
            }
        }
    }



}

