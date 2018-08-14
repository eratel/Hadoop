package com.abhsy.ordertopn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
public class OrderTonN {

    public static class IndexStepOneMapper extends Mapper<LongWritable,Text,OrderBean,Text> {
        OrderBean orderBean = new OrderBean();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] split = line.split(",");
            Double money = Double.parseDouble(split[2]);
            String tickno = split[0];
            orderBean.setMoney(money);
            orderBean.setOrderNo(tickno);
            context.write(orderBean,new Text(split[0]));
        }
    }


    public static class  IndexStepOneReduce extends Reducer<OrderBean,Text,Text,OrderBean>{
        @Override
        protected void reduce(OrderBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text text:
            values) {
                context.write(text,key);
            }
        }
    }

    public static void main(String [] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        job.setJarByClass(IndexStepOneMapper.class);
        job.setMapperClass(IndexStepOneMapper.class);
        job.setReducerClass(IndexStepOneReduce.class);
        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(Text.class);
        job.setNumReduceTasks(1);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(OrderBean.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job,new Path("E:/技术栈/六期大数据/文档/就业班/04/作业题、案例数据/3/orders.txt"));
        FileOutputFormat.setOutputPath(job,new Path("E:/技术栈/六期大数据/文档/就业班/04/作业题、案例数据/3/output"));
        boolean res = job.waitForCompletion(true);
        System.exit(res?0:1);
    }
}



























