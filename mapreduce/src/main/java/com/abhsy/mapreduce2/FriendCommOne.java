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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * @program: abhsy-hadoop
 * @author: jikai.sun
 * @create: 2018-08-13
 **/
/**
 *  f1 ; p1 f1:p2
 *  通过拿到所有friend对应的person，然后通过reduce分组friend，就可以拿到一个friend对应的person有哪些
 */
public class FriendCommOne {

    public static class IndexStepOneMapper extends Mapper<LongWritable,Text,Text,Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] split = line.split(":");
            String person = split[0];
            String[] friend = split[1].split(",");
            for (String f:
                 friend) {
                context.write(new Text(f),new Text(person));
            }

        }
    }

    /**
     * 拿到persoon共同好友
     */
    public static class  IndexStepOneReduce extends Reducer<Text,Text,Text,Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> persons, Context context) throws IOException, InterruptedException {
            StringBuffer stringBuffer = new StringBuffer();
            for (Text person :
                    persons) {
                stringBuffer.append(person).append(",");
            }
            context.write(new Text(key),new Text(stringBuffer.toString()));
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
        job.setJarByClass(IndexStepOneMapper.class);
        job.setMapperClass(IndexStepOneMapper.class);
        job.setReducerClass(IndexStepOneReduce.class);
        //在数据拉取之前就做Reduce操作
        // 对MAP操作进行局部聚合，提高网络IO操作，大量KV数据传送          读音【康版纳】
//        job.setCombinerClass(IndexStepOneReduce.class);
        //添加MAP输入值的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        //ReduceTask为3 会生成3个文件
        job.setNumReduceTasks(1);
        //设置整个输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //设置数据读取组件,结果输出组件
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        //设置处理文件路径  FileInputFormat要使用reduce包下面的
        FileInputFormat.setInputPaths(job,new Path("E:/技术栈/六期大数据/文档/就业班/04/作业题、案例数据/common-friends1.log"));
        FileOutputFormat.setOutputPath(job,new Path("E:/技术栈/六期大数据/文档/就业班/04/作业题、案例数据/output"));
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



























