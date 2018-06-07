package com.barry.hadoop.hdfs.utils;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.StringBuilderWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * @Auther: hants
 * @Date: 2018-06-05 13:05
 * @Description: hdfs client demo
 */
public class HdfsClientDemo {

    private static final Logger logger = LoggerFactory.getLogger(HdfsClientDemo.class);

    private FileSystem fs = null;

    @Before
    public void init() throws Exception {
        Configuration conf = new Configuration();
        conf.set("dfs.replication", "3");
        conf.set("dfs.blocksize", "128M");
        fs = FileSystem.get(new URI("hdfs://192.168.33.101:9000/"), conf, "hts");
    }

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
                if (fs!=null){
                    fs.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    /**
     * 下载文件
     * */
    @Test
    public void testGetHdfsSystem() throws Exception {
        fs.copyToLocalFile(new Path("/data/pkgfiles/hbase-1.2.6-bin.tar.gz"), new Path("C:\\Users\\qxv0963\\Desktop\\TempFiles"));
    }

    /**
     * 在hdfs内部移动修改名称
     * */
    @Test
    public void testRename() throws Exception {
        fs.rename(new Path("/jdk-8u171-linux-x64.tar.gz"), new Path("/data/pkgfiles/jdk8.tar.gz"));
    }

    /**
     * 在hdfs内部创建文件夹
     * */
    @Test
    public void testMkdir() throws Exception {
        fs.mkdirs(new Path("/data/busifiles"));
    }

    /**
     * 在hdfs内部删除文件夹
     * true代表递归删除
     * */
    @Test
    public void testRm() throws Exception {
        fs.delete(new Path("/data/busifiles"), true);
    }

    /**
     * 查询hdfs指定目录下的信息
     * true代表递归显示
     * */
    @Test
    public void testLs() throws Exception {
        //只显示文件信息，不返回文件夹的目录信息
        RemoteIterator<LocatedFileStatus> remoteIterator = fs.listFiles(new Path("/data/pkgfiles"), true);
        while (remoteIterator.hasNext()){
            LocatedFileStatus status = remoteIterator.next();
            logger.info("===============================>块大小:"+status.getBlockSize());
            logger.info("===============================>文件长度:"+status.getBlockSize());
            logger.info("===============================>副本数量:"+status.getReplication());
            logger.info("===============================>块信息:"+ Arrays.toString(status.getBlockLocations()));
            logger.info("<==========================================================>");
        }
    }

    @Test
    public void testLs2() throws Exception {
        //返回hdfs内部文件和文件夹的信息
        FileStatus[] fileStatuses = fs.listStatus(new Path("/"));
        for (FileStatus status : fileStatuses){
            logger.info("===============================>文件全路径:"+status.getPath());
            if (status.isDirectory()){
                logger.info("===============================>文件夹类型");
            }else {
                logger.info("===============================>文件类型");
            }
            logger.info("===============================>块大小:"+status.getBlockSize());
            logger.info("===============================>文件长度:"+status.getBlockSize());
            logger.info("===============================>副本数量:"+status.getReplication());
            logger.info("<==========================================================>");
        }
    }

    /**
     * 读取hdfs中的文件内容
     * */
    @Test
    public void testOpen() throws Exception {
        FSDataInputStream fsDataInputStream = fs.open(new Path("/data/calfiles/bmwtest.txt"));
        StringWriter stringWriter = new StringWriter();
        IOUtils.copy(fsDataInputStream, stringWriter, StandardCharsets.UTF_8.toString());
        String content = stringWriter.toString();
        logger.info("=============================>文件内容为:"+content);
        fsDataInputStream.close();
        stringWriter.close();
    }

    @Test
    public void testOpen2() throws Exception {
        FSDataInputStream fsDataInputStream = fs.open(new Path("/data/calfiles/bmwtest.txt"));
        StringBuilderWriter stringWriter = new StringBuilderWriter();
        IOUtils.copy(fsDataInputStream, stringWriter, StandardCharsets.UTF_8.toString());
        String content = stringWriter.toString();
        logger.info("=============================>文件内容为:"+content);
        fsDataInputStream.close();
        stringWriter.close();
    }

    @Test
    public void testOpen3() throws Exception {
        FSDataInputStream fsDataInputStream = fs.open(new Path("/data/calfiles/bmwtest.txt"));
        BufferedReader br = new BufferedReader(new InputStreamReader(fsDataInputStream));
        String line = null;
        while ((line=br.readLine())!=null){
            logger.info("=============================>文件内容为:"+line);
        }
        fsDataInputStream.close();
        br.close();
    }

    /**
     * 读取hdfs中指定偏移量的文件内容
     * */
    @Test
    public void testRandomData() throws Exception {
        FSDataInputStream fsDataInputStream = fs.open(new Path("/data/calfiles/bmwtest.txt"));
        //将读取的起始位置进行指定
        fsDataInputStream.seek(82);
        //读5个字节
        byte[] bytes = new byte[5];
        fsDataInputStream.read(bytes);
        logger.info("=====================>读取内容为:"+new String(bytes));
        fsDataInputStream.close();
    }

    @Test
    public void testWriteData() throws Exception {
        //fs.mkdirs(new Path("/data/images"));
        //创建一个图片文件,此图片未填充内容，因此不能正常显示出来
        FSDataOutputStream out = fs.create(new Path("/data/images/validPicture.jpg"), false);
        //往图片中写入内容
        FileInputStream inputStream = new FileInputStream("C:\\Users\\qxv0963\\Desktop\\TempFiles\\jpg\\zly.jpg");
        byte[] bytes = new byte[1024];
        int read = 0;
        while ((read = inputStream.read(bytes))!=-1){
            out.write(bytes, 0, read);
        }
        inputStream.close();
        out.close();
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

