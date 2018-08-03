package com.abhsy.hdfs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import java.io.IOException;

/**
 * @program: hadoop
 * @author: jikai.sun
 * @create: 2018-07-30
 **/
public class Hdfs {

    public static void main(String arg[]) throws IOException {
//        如果没有配置环境变量可以添加Property
//        System.setProperty("hadoop.home.dir", "D:/hadoop-2.6.1");
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://hadoop1:9000");
//        FileSystem是抽象的  无法New(很多厂家的实现So不能NEW会有其他厂家的实现)
        FileSystem fs = FileSystem.get(conf);
        //recursive 是否递归
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), false);
        //通过迭代器拿到hdfs文件系统的文件
        while (listFiles.hasNext()){
            LocatedFileStatus fileStatus = listFiles.next();
            Path path = fileStatus.getPath();
            String name = path.getName();
            System.out.printf("---------------" + name);
        }
    }
}























