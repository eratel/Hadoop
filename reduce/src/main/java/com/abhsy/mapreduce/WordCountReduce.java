package com.abhsy.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @program: abhsy-hadoop
 * @author: jikai.sun
 * @create: 2018-08-06
 **/

/**
 * 输入就是map的输出
 * 输出看自己的业务逻辑
 */
public class WordCountReduce extends Reducer<Text, IntWritable, Text, IntWritable>{

    /**
     * key为map输出的单词
     * values为map输出的个数
     * 得到以KEY为分组的，迭代器Values数据
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        for (IntWritable v:
        values) {
            count += v.get();
        }
        context.write(key,new IntWritable(count));
    }
}



















































































