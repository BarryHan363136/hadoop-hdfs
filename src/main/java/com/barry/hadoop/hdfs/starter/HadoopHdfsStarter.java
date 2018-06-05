package com.barry.hadoop.hdfs.starter;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.context.annotation.ComponentScan;

/**
 * @Auther: hants
 * @Date: 2018-06-05 12:52
 * @Description: springboot starter
 */
@SpringBootApplication
@ComponentScan(basePackages={"com.barry.hadoop"})
@EnableAutoConfiguration(exclude={DataSourceAutoConfiguration.class})
public class HadoopHdfsStarter {

    public static void main(String[] args) {
        SpringApplication.run(HadoopHdfsStarter.class, args);
    }

}