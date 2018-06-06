package com.barry.hadoop.hdfs.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * @Auther: hants
 * @Date: 2018-06-06 11:07
 * @Description: hdfs words count
 */
public class HdfsWordsCountTest {

    private static final Logger logger = LoggerFactory.getLogger(HdfsWordsCountTest.class);

    private FileSystem fs = null;

    @Before
    public void init() throws Exception {
        Configuration conf = new Configuration();
        conf.set("dfs.replication", "3");
        conf.set("dfs.blocksize", "128M");
        fs = FileSystem.get(new URI("hdfs://192.168.33.101:9000/"), conf, "hts");
    }

    /**
     * 读取hdfs中的文件，并统计文件中的wordcount,并输出统计结果到hdfs中的文件中
     * */
    @Test
    public void testWordsCount() throws Exception {
        Properties properties = new Properties();
        properties.load(HdfsWordsCountTest.class.getClassLoader().getResourceAsStream("job.properties"));
        Class<?> mapper_class = Class.forName(properties.getProperty("MAPPER_CLASS"));
        Mapper mapper = (Mapper) mapper_class.newInstance();

        Context context = new Context();

        //去hdfs中读文件，一次读一行
        RemoteIterator<LocatedFileStatus> iter = fs.listFiles(new Path("/data/wordscount"), false);
        while (iter.hasNext()){
            LocatedFileStatus file = iter.next();
            FSDataInputStream inputStream = fs.open(file.getPath());
            BufferedReader br = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
            String line;
            while ((line=br.readLine())!=null){
                //调用一个方法对每一行进行业务处理
                mapper.map(line, context);
            }
            inputStream.close();
            br.close();
        }
        //输出结果,输出到hdfs结果文件wcrep.text
        HashMap<Object, Object> contextMap = context.getContextMap();
        FSDataOutputStream outputStream = fs.create(new Path("/data/wordscount/wcrep.text"), false);
        Set<Map.Entry<Object, Object>> entrySet = contextMap.entrySet();
        for (Map.Entry entry : entrySet){
            outputStream.write((entry.getKey().toString()+"\t"+entry.getValue()+"\n").getBytes());
        }
        //关闭
        outputStream.close();
        logger.info("=============================>数据统计完成!");
    }






    @After
    public void close(){
        try {
            if (fs!=null){
                fs.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}

