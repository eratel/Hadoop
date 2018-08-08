package com.abhsy.flowsum;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @program: abhsy-hadoop
 * @author: jikai.sun
 * @create: 2018-08-08
 **/
public class FlowSumApp {

    public static class flowSumMapper extends Mapper<LongWritable,Text,Text,FlowBean>{
        Text k= new Text();
        FlowBean v = new FlowBean();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            //根据分隔符切开
            String[] fields = StringUtils.split(line, "\t");
            String phoneNum = fields[1];
            long upFlow = Long.parseLong(fields[fields.length -3]);
            long downFlow = Long.parseLong(fields[fields.length -2]);
            k.set(phoneNum);
            v.set(upFlow, downFlow);
            context.write(k,v);
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

    public static void main(String [] agrs){

    }
}













































































