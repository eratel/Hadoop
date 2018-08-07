package com.abhsy.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @program: abhsy-hadoop
 * @author: jikai.sun
 * @create: 2018-08-06
 *
 *
 *     Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
 *     KEYIN：是指框架读取到的数据的key类型
 *     		在默认的读取数据组件InputFormat下，读取的key是一行文本的偏移量，所以key的类型是long类型的
 *
 *     VALUEIN指框架读取到的数据的value类型
 *     		在默认的读取数据组件InputFormat下，读到的value就是一行文本的内容，所以value的类型是String类型的
 *
 *     KEYOUT是指用户自定义逻辑方法返回的数据中key的类型 这个是由用户业务逻辑决定的。
 *     		在我们的单词统计当中，我们输出的是单词作为key，所以类型是String
 *
 *     VALUEOUT是指用户自定义逻辑方法返回的数据中value的类型 这个是由用户业务逻辑决定的。
 *     		在我们的单词统计当中，我们输出的是单词数量作为value，所以类型是Integer
 *
 *    但是，String ,Long都是jdk中自带的数据类型，在序列化的时候，效率比较低。hadoop为了提高序列化的效率，他就自己自定义了一套数据结构。
 *    所以说在我们的hadoop程序中，如果该数据需要进行序列化（写磁盘，或者网络传输），就一定要用实现了hadoop序列化框架的数据类型
 *
 *    Long------->LongWritable
 *    String----->Text
 *    Integer---->IntWritable
 *    null------->nullWritable
 **/
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    /**
     * 每一行调用一次 map方法
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] split = line.split(" ");
        for (String s:
             split) {
            context.write(new Text(s),new IntWritable(1));
        }
    }
}













































































