package com.abhsy.flowsum;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @program: abhsy-hadoop
 * @author: jikai.sun
 * @create: 2018-08-08
 * Hadoop默认的排序算法，只会针对key值进行排序，按照字典顺序排序
 **/
public class FlowSumApp {

    public static class flowSumMapper extends Mapper<LongWritable,Text,Text,FlowBean>{
        Text k= new Text();
        FlowBean v = new FlowBean();

        /**
         * 每一行数据调用一次map方法拿到的数据
         * 可以理解为拿到为reduce 所需要的数据，例如现在需要统计手机号下面的流量
         * 下面的map方法就是把hdfs文件中sumflow文件中数据全部读取，并拿到每一行中的 手机号，流量，写入到文件中
         * reduce会自动拉去map写好的文件，然后在通过HASH取模运算,找到需要（两台%2三台%3）拉取数据的机器，拉取到数据后首先放到本地磁盘
         * ，盘内数据按照KEY进行分组。
         * 然后对每一组数据运行reduce方法。然后通过TextOutPutFormat输出到文件中就OK了
         * ↑以上就是整个MapReduce过程
         */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if(StringUtils.isNotBlank(value.toString())){
                //根据分隔符切开
                String[] fields = StringUtils.split(line, "\t");
                if(fields.length > 1){
                    String phoneNum = fields[1];
                    long upFlow = Long.parseLong(fields[fields.length -3]);
                    long downFlow = Long.parseLong(fields[fields.length -2]);
                    k.set(phoneNum);
                    v.set(upFlow, downFlow);
                    context.write(k,v);
                }
            }
        }
    }

    public static class flowSumReduce extends Reducer<Text,FlowBean,Text,FlowBean>{
        FlowBean v = new FlowBean();
        //这里reduce方法接收到的key就是某一组《a手机号，bean》《a手机号，bean》   《b手机号，bean》《b手机号，bean》当中的第一个手机号
        //这里reduce方法接收到的values就是这一组kv对中的所以bean的一个迭代器
        @Override
        protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
            long upflowCount = 0;
            long downflowCount = 0;
            for (FlowBean flowBean:
                 values) {
                upflowCount += flowBean.getUpflow();
                downflowCount += flowBean.getDownflow();
            }
            v.set(upflowCount,downflowCount);
            context.write(key,v);
        }
    }

    public static void main(String [] agrs) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://hadoop1:9000");
        System.setProperty("HADOOP_USER_NAME", "root");
        Job job = Job.getInstance(configuration);
        job.setJarByClass(FlowSumApp.class);
        job.setMapperClass(flowSumMapper.class);
        job.setReducerClass(flowSumReduce.class);
//        如果跟最后输入类型相同可不配置
//        job.setMapOutputKeyClass(Text.class);
//        job.setMapOutputValueClass(FlowBean.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);
//      默认使用TextInputFormat 可不配置
//        job.setInputFormatClass(TextInputFormat.class);
//        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job,new Path("/sumflow"));
        FileOutputFormat.setOutputPath(job,new Path("/output"));
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













































































