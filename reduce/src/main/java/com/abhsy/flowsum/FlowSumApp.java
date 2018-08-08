package com.abhsy.flowsum;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

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

    public static void main(String [] agrs){

    }
}
