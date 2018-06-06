package com.barry.hadoop.hdfs.utils;

import java.util.HashMap;
import java.util.Map;

/**
 * @Auther: hants
 * @Date: 2018-06-06 11:28
 * @Description: Context
 */
public class Context {

    private HashMap<Object, Object> contextMap = new HashMap<Object, Object>();

    public void write(Object key, Object value){
        contextMap.put(key, value);
    }

    public Object get(Object key){
        return contextMap.get(key);
    }

    public HashMap<Object, Object> getContextMap(){
        return contextMap;
    }


}

