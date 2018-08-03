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

    public static void main(String[] agrs) throws IOException {
//        如果没有配置环境变量可以添加Property
//        System.setProperty("hadoop.home.dir", "D:/hadoop-2.6.1");
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://hadoop1:9000");
        /**
         * 设置自己的用户权限
         */
        System.setProperty("HADOOP_USER_NAME", "root");

//        FileSystem是抽象的  无法New(很多厂家的实现So不能NEW会有其他厂家的实现)
        FileSystem fs = FileSystem.get(conf);
        Hdfs hdfs = new Hdfs();
//        hdfs.listFiles(fs);
//        hdfs.copyFromLocalFile(fs);
        hdfs.copyToLocalFile(fs);
    }

    public static void listFiles(FileSystem fs) throws IOException {
        //recursive 是否递归
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), false);
        //通过迭代器拿到hdfs文件系统的文件
        while (listFiles.hasNext()) {
            LocatedFileStatus fileStatus = listFiles.next();
            Path path = fileStatus.getPath();
            String name = path.getName();
            System.out.printf("---------------" + name);
        }
    }


    public void copyFromLocalFile(FileSystem fs) throws IOException {
        fs.copyFromLocalFile(new Path("D://hadoop-2.6.1//etc//hadoop"), new Path("/"));
    }

    /**
     *写入windows平台文件
     */
    public void copyToLocalFile(FileSystem fs) throws IOException {
        //delsrc 代表是否会删除hdfs上面的文件
        //使用这个方法必须需要添加winutils.exe  windows下编译过的hadoop 配置环境变量
        fs.copyToLocalFile(new Path("/hadoop"), new Path("C://"));
        //useRawLocalFileSystem 代表是否使用当前系统的FileSystem  当前为Windows的FileSystem
//        fs.copyToLocalFile(false,new Path("/hadoop"), new Path("C://"),true);
        fs.close();
    }

}























