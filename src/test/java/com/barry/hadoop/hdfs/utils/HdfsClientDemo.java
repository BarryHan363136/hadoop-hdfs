package com.barry.hadoop.hdfs.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.net.URI;
/**
 * @Auther: hants
 * @Date: 2018-06-05 13:05
 * @Description: hdfs client demo
 */
public class HdfsClientDemo {

    private static final Logger logger = LoggerFactory.getLogger(HdfsClientDemo.class);

    @Test
    public void testUploadFilesToHdfsSystem(){
        FileSystem fs = null;
                /**
                 * new Configuration()会从项目的classpath中加载core-default.xml,hdfs-default.xml,core-site.xml.hdfs.site.xml等文件
                 * Configuration参数对象的加载机制：
                 * (1).构造时会加载jar包中的默认配置，xx-default.xml
                 * (2).再加载用户配置的xx-site.xml，覆盖掉默认值
                 * (3).构造完成之后,还可以conf.set(name,value),会再次覆盖用户配置文件中的参数值
                 * */
        Configuration conf = new Configuration();
        //1.指定客户端上传文件到hdfs时需要保存的副文本数为3
        conf.set("dfs.replication", "3");
        //2.设置客户端上传文件到hdfs时切块的规格大小
        conf.set("dfs.blocksize", "128M");
        try {
            /**
             * 构造一个访问指定hdfs系统的客户端对象
             * 参数1: HDFS系统的URI
             * 参数2：客户端需要特别指定的参数
             * 参数3：客户端的身份(用户名)
             * */
            fs = FileSystem.get(new URI("hdfs://192.168.33.101:9000/"), conf, "hts");

            //上传文件到hdfs里
            fs.copyFromLocalFile(new Path("C:\\Resources\\TechDocs\\hbase\\hbase\\hbase-1.2.6-bin.tar.gz"), new Path("/data/pkgfiles"));

        } catch (Exception e) {
            logger.error("testHdfsFileSystem error {} ", e);
        }finally {
            try {
                fs.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }























}

