package com.abhsy.mapreduce2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * @program: abhsy-hadoop
 * @author: jikai.sun
 * @create: 2018-08-13
 **/
public class FriendCommTwo {

    public static class IndexStepOneMapper extends Mapper<LongWritable,Text,Text,Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String lin = value.toString();
            String[] split = lin.split("\t");
            String friend = split[0];
            String[] persons = split[1].split(",");
            //length -1 因为从下标开始
            for (int i = 0; i < persons.length -1;i ++){
                // i + 1 必须要加i
                for (int j = 1; j < persons.length - i ;j ++){
                    context.write(new Text(persons[i]+"-"+persons[j]), new Text(friend));
                }
            }
        }
    }


    public static class  IndexStepOneReduce extends Reducer<Text,Text,Text,Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuffer stringBuffer = new StringBuffer();
            for (Text friend:
                 values) {
                stringBuffer.append(friend).append(",");
            }
            context.write(key,new Text(stringBuffer.toString()));
        }
    }

    public static void main(String [] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        job.setJarByClass(IndexStepOneMapper.class);
        job.setMapperClass(IndexStepOneMapper.class);
        job.setReducerClass(IndexStepOneReduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setNumReduceTasks(1);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        //设置处理文件路径  FileInputFormat要使用reduce包下面的
        FileInputFormat.setInputPaths(job,new Path("E:/技术栈/六期大数据/文档/就业班/04/作业题、案例数据/output"));
        FileOutputFormat.setOutputPath(job,new Path("E:/技术栈/六期大数据/文档/就业班/04/作业题、案例数据/output2"));
        boolean res = job.waitForCompletion(true);
        System.exit(res?0:1);
    }
}



























