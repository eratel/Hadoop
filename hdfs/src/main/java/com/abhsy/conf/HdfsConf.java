package com.abhsy.conf;

import org.apache.hadoop.conf.Configuration;
import org.springframework.context.annotation.Bean;

/**
 * @program: abhsy-hadoop
 * @author: jikai.sun
 * @create: 2018-08-03
 **/
@org.springframework.context.annotation.Configuration
public class HdfsConf {
    @Bean
    public Configuration Configuration(){
        return new Configuration();
    }
}
