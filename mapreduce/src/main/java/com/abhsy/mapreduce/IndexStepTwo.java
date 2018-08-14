package com.abhsy.mapreduce;

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
public class IndexStepTwo {

    public static class IndexStepTwoMapper extends Mapper<LongWritable,Text,Text,Text> {
        Text k = new Text();
        Text v = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            //reduce输出是\t所以按照制表符分割
            String[] filelds = line.split("\t");
            String word_file = filelds[0];
            String count = filelds[1];
            String[] split = word_file.split("--");
            String word = split[0];
            String file = split[1];
            k.set(word);
            v.set(file + "--" + count);
            context.write(k,v);
        }
    }

    public static class  IndexStepTwoReduce extends Reducer<Text,Text,Text,Text>{
        Text v = new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuffer stringBuffer = new StringBuffer();
            for (Text t :
                    values) {
                stringBuffer.append(t.toString() + " ");
            }
            v.set(stringBuffer.toString());
            context.write(key,v);
        }
    }

    public static void main(String [] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
//        configuration.set("fs.defaultFS", "hdfs://hadoop1:9000");
//        配置使用yarn文件管理 如果不配置默认使用本机的
//        configuration.set("mapreduce.framework.name", "yarn");
//        configuration.set("yarn.resourcemanager.hostname", "hadoop1");
//        System.setProperty("HADOOP_USER_NAME", "root");
        Job job = Job.getInstance(configuration);
        job.setJarByClass(IndexStepTwoMapper.class);
        job.setMapperClass(IndexStepTwoMapper.class);
        job.setReducerClass(IndexStepTwoReduce.class);
        //在数据拉取之前就做Reduce操作
        // 对MAP操作进行局部聚合，提高网络IO操作，大量KV数据传送          读音【康版纳】
//        job.setCombinerClass(IndexStepOneReduce.class);
        //添加MAP输入值的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        //ReduceTask为3 会生成3个文件
        job.setNumReduceTasks(3);
        //设置整个输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //设置数据读取组件,结果输出组件
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        //设置处理文件路径  FileInputFormat要使用reduce包下面的
        FileInputFormat.setInputPaths(job,new Path("E:/技术栈/六期大数据/文档/就业班/04/作业题、案例数据/1/output"));
        FileOutputFormat.setOutputPath(job,new Path("E:/技术栈/六期大数据/文档/就业班/04/作业题、案例数据/1/output1"));
        /**
         * 等待客户端返回是否完成
         */
        boolean res = job.waitForCompletion(true);
        /**
         *  如果返回true就退出
         */
        System.exit(res?0:1);
    }
}



























